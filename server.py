import socket
import argparse
import asyncio
import signal
import json
import urllib.parse
import traceback
import hashlib
from collections import OrderedDict

from database import Database
import constants as const

APP_NAME = 'pii-service'
__version__ = '1.0'


class Context:
    """
    Service scope context object
    """

    def __init__(self, loop, db_ro: Database, db_rw_pool: asyncio.Queue, rate_limit_buckets):
        self.db_ro = db_ro
        self.db_rw_pool = db_rw_pool
        self.loop = loop
        self.rate_limit_buckets = rate_limit_buckets


async def serve(loop):
    """ Service main
    """
    rate_limit_buckets = {}
    # start rate_limit_buckets periodic refills
    loop.create_task(refill_rate_limit_buckets(rate_limit_buckets,
                                               interval_sec=const.RATE_LIMIT_SEC,
                                               limit=const.RATE_LIMIT_NUM))
    # create ro db used by GETs
    db_ro = Database(loop, const.DB_RO_URI)
    # rw db pool used by mutable methods, unbound grow with low limited shrink
    db_rw_pool = asyncio.Queue()  # unbound on get, but manually limited to const.db_rw_pool_SIZE on put
    for i in range(10):  # pre-fill few initially
        await db_rw_pool.put(Database(loop, const.DB_RW_URI))
    # run DB DDL script
    db = await db_rw_pool.get()
    await db.executescript(const.DB_DDL_SQL)  # create table if missing etc
    await db_rw_pool.put(db)  # put db back to pool
    context = Context(loop, db_ro, db_rw_pool, rate_limit_buckets)
    # setup ipv4 TCP socket server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # enable TCP socket keep-alive
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Linux specific socket options
        assert (hasattr(socket, "TCP_KEEPIDLE") and hasattr(socket, "TCP_KEEPINTVL") and hasattr(socket, "TCP_KEEPCNT"))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, const.TCP_KEEPIDLE_SEC)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, const.TCP_KEEPINTVL_SEC)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, const.TCP_KEEPCNT_SEC)

        print(f"Listening on {const.HOST}:{const.PORT}")

        s.bind((const.HOST, const.PORT))
        s.listen(const.TCP_LISTEN_BACKLOG)
        s.setblocking(False)
        while True:
            conn, addr = await loop.sock_accept(s)
            print("Connected by", addr)
            conn.setblocking(False)
            loop.create_task(handle_connection(conn, addr, context))


async def handle_connection(conn_sock, addr, context: Context) -> None:
    print(f"handle_connection {addr}: begin")
    rate_limit_buckets = context.rate_limit_buckets
    loop = context.loop
    current_db = context.db_ro  # start with RO then switch RW as needed
    with conn_sock:
        try:
            buffer = ''
            while True:
                data = await loop.sock_recv(conn_sock, const.HTTP_BUFFER_SIZE)
                if data == b'':
                    break  # connection closed
                data_str = data.decode("utf-8")
                data_str = data_str.replace("\r", "")
                buffer += data_str
                end_of_header = buffer.find('\n\n')  # we support HTTP 1.1 pipelining
                if end_of_header < 0:
                    continue
                request = buffer[:end_of_header]
                buffer = buffer[end_of_header + 2:]
                print(f"Request: \n{request}\n\n")
                # reta limiter
                tokens = rate_limit_buckets.get(addr[0], const.RATE_LIMIT_NUM)
                if tokens > 0:
                    rate_limit_buckets[addr[0]] = tokens - 1
                else:
                    # Too many requests
                    await loop.sock_sendall(conn_sock, RESPONSE_429) # Too many requests sent
                    return
                # get first line
                lines = request.split("\n")
                method, req_path, version = lines[0].split(" ")
                # basic checks
                if method not in {"GET", "PUT", "PATCH", "DELETE"}:
                    # Bad Request
                    await loop.sock_sendall(conn_sock, RESPONSE_400)
                    break  # close connection
                if version == "HTTP/1.0":
                    # HTTP Version not supported
                    await loop.sock_sendall(conn_sock, RESPONSE_505)
                    break  # close connection
                if "content-length:" in request.lower():
                    # Bad Request, content in requests is not supported
                    await loop.sock_sendall(conn_sock, RESPONSE_400)
                    break  # close connection
                # check and switch to RW db if needed
                if method != "GET" and current_db is context.db_ro:
                    current_db = await context.db_rw_pool.get()
                # process request
                response, keep_open = await process_request(method, req_path, current_db)
                print(f"Response: \n{response.decode('utf-8')}")
                # send response
                await loop.sock_sendall(conn_sock, response)
                if not keep_open:
                    break  # close connection
        except Exception:
            print(traceback.format_exc())
        finally:
            conn_sock.shutdown(socket.SHUT_RDWR)
            # connection is getting closed, do rollback if needed
            if not current_db is context.db_ro:
                if await current_db.in_transaction():
                    await current_db.execute("ROLLBACK")
    # return rw db back to pull or just discard it if pool is big enough
    if not current_db is context.db_ro and context.db_rw_pool.qsize() < const.DB_RW_POOL_SIZE:
        await context.db_rw_pool.put(current_db)
    conn_sock.close()
    print(f"handle_connection {addr}: closed")


def compose_response(status: int, json: str = '') -> bytes:
    content = json.encode("utf-8")
    length = len(content)
    header = f'HTTP/1.1 {status}\n'
    header += 'Connection: keep-alive\n'
    if json:
        header += 'Content-Type: application/json; charset=utf-8\n'
        header += 'Cache-control: no-cache, no-store, must-revalidate\n'
    header += f'Content-Length: {length}\n\n'
    response = header.encode("utf-8") + content
    return response


async def process_request(method: str, req_path: str, db: Database) -> tuple:
    """
    :return: tuple (bytes, bool) - (response, keep connection open)
    """
    url = urllib.parse.urlparse(req_path)
    query = dict((k, v[0] if isinstance(v, list) else v) for k, v in urllib.parse.parse_qs(url.query).items())
    path = url.path
    try:
        if path == "/ping":
            return await handle_ping(method, query)
        elif path == "/db/begin":
            return await handle_db_begin(method, db)
        elif path == "/db/commit":
            return await handle_db_commit(method, db)
        elif path == "/db/rollback":
            return await handle_db_rollback(method, db)
        elif path == "/pii/search":
            return await handle_pii_search(method, query, db)
        elif path == "/pii/insert":
            return await handle_pii_insert(method, query, db)
        elif path == "/pii/update":
            return await handle_pii_update(method, query, db)
        elif path == "/pii/delete":
            return await handle_pii_delete(method, query, db)
        else:
            response = RESPONSE_404, False  # Not Found
    except Exception as ex:
        result_json = json.dumps({"Error": str(ex)}, indent=4)
        response = compose_response(500, result_json + '\n'), False
    return response


async def handle_ping(method: str, query: dict) -> tuple:
    if method != "GET":
        return RESPONSE_400, False  # Bad Request
    await asyncio.sleep(1)
    response = RESPONSE_200, True  # OK
    return response


async def handle_db_begin(method: str, db: Database) -> tuple:
    if method != "PUT":
        return RESPONSE_400, False  # Bad Request
    await db.execute("BEGIN")
    response = RESPONSE_200, True  # OK
    return response


async def handle_db_commit(method: str, db: Database) -> tuple:
    if method != "PUT":
        return RESPONSE_400, False  # Bad Request
    await db.execute("COMMIT")
    response = RESPONSE_200, True  # OK
    return response


async def handle_db_rollback(method: str, db: Database) -> tuple:
    if method != "PUT":
        return RESPONSE_400, False  # Bad Request
    await db.execute("ROLLBACK")
    response = RESPONSE_200, True  # OK
    return response


async def handle_pii_search(method: str, query: dict, db: Database) -> tuple:
    if method != "GET":
        return RESPONSE_400, False  # Bad Request




    response = RESPONSE_200, True  # OK
    return response


def encode_SSN(ssn: str) -> str:
    return hashlib.blake2b(ssn.encode('utf8'), salt=const.HMAC_SALT).hexdigest()


async def handle_pii_insert(method: str, query: dict, db: Database) -> tuple:
    if method != "PUT":
        return RESPONSE_400, False  # Bad Request
    query_keys = set(query.keys())
    fields = ", ".join(const.DB_PII_TABLE_FIELDS)
    if set(const.DB_PII_TABLE_FIELDS) != set(query_keys):
        result_json = json.dumps({"Error": f"Invalid list of fields {query_keys}." +
                                           f"Expected: {const.DB_PII_TABLE_FIELDS}"}, indent=4)
        response = compose_response(400, result_json + '\n'), True  # OK
        return response
    values = ":" + ", :".join(const.DB_PII_TABLE_FIELDS)
    sql = f"""
    INSERT INTO  pii_table ({fields})
    VALUES ({values})
    RETURNING {fields};         
    """
    # encode SSN
    if "SSN" in query_keys:
        query['SSN'] = encode_SSN(query['SSN'])
    result = await db.execute(sql, query)
    new_result = []
    for i, rec in enumerate(result):
        new_rec = OrderedDict()
        for k, v in zip(const.DB_PII_TABLE_FIELDS, rec):
            new_rec[k] = v
        new_result.append(new_rec)
    result_json = json.dumps(new_result, indent=4)
    response = compose_response(200, result_json + '\n'), True  # OK
    return response


async def handle_pii_update(method: str, query: dict, db: Database) -> tuple:
    if method != "PATCH":
        return RESPONSE_400, False  # Bad Request
    response = RESPONSE_200, True  # OK
    return response


async def handle_pii_delete(method: str, query: dict, db: Database) -> tuple:
    if method != "DELETE":
        return RESPONSE_400, False  # Bad Request
    response = RESPONSE_200, True  # OK
    return response


async def refill_rate_limit_buckets(buckets: dict, interval_sec: int, limit: int) -> None:
    while True:
        await asyncio.sleep(interval_sec)
        for key in buckets.keys():
            buckets[key] = limit  # TODO: shrink buckets based on last activity time, extract into class/func


def shutdown():
    print('Got a SIGINT!')
    tasks = asyncio.all_tasks()
    print(f'Cancelling {len(tasks)} task(s).')
    [task.cancel() for task in tasks]


def main():
    parser = argparse.ArgumentParser(description=f"{APP_NAME} v{__version__}")
    parser.parse_args()
    #
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.add_signal_handler(signal.SIGINT, shutdown)
    #
    try:
        loop.create_task(serve(loop))
        loop.run_forever()
    finally:
        loop.close()
    print("Finished")


RESPONSE_200 = compose_response(200)  # OK
RESPONSE_400 = compose_response(400)  # Bad Request
RESPONSE_404 = compose_response(404)  # Not Found
RESPONSE_429 = compose_response(429)  # Too many requests sent
RESPONSE_505 = compose_response(505)  # HTTP Version Not Supported
RESPONSE_500 = compose_response(500)  # Internal Server Error

if __name__ == "__main__":
    main()

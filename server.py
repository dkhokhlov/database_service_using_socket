import socket
import argparse
import asyncio
import signal
import functools
import json
import urllib.parse
import traceback
import time
from database import Database
import constants as const
import utils

APP_NAME = 'pii-service'
__version__ = '1.0'

g_shutdown = 0


class Context:
    """
    Service scope context object
    """

    def __init__(self, loop, server_socket, db_ro: Database, db_rw_set_active: set, db_rw_set_idle: set,
                 rate_limit_buckets):
        self.server_socket = server_socket
        self.db_ro = db_ro
        self.db_rw_set_active = db_rw_set_active
        self.db_rw_set_idle = db_rw_set_idle
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
    # rw db pools used by mutable methods
    db_rw_set_idle = set()
    db_rw_set_active = set()
    for i in range(10):  # pre-fill few initially
        db_rw_set_idle.add(Database(loop, const.DB_RW_URI))
    # run DB DDL script
    db = db_rw_set_idle.pop()
    await db.executescript(const.DB_DDL_SQL)  # create table if missing etc
    db_rw_set_idle.add(db)  # put db back to pool
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
        # start shutdown watchdog - will close server socket and pools
        context = Context(loop, s, db_ro, db_rw_set_active, db_rw_set_idle, rate_limit_buckets)
        loop.create_task(shutdown_watchdog(context, 0.2))
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
                    await loop.sock_sendall(conn_sock, utils.RESPONSE_429)  # Too many requests sent
                    return
                # check connections limit
                if len(asyncio.all_tasks(loop)) - 2 > const.HTTP_CONNECT_MAX:
                    await loop.sock_sendall(conn_sock, utils.RESPONSE_503)  # Too many connections
                    return
                # get first line
                lines = request.split("\n")
                method, req_path, version = lines[0].split(" ")
                # basic checks
                if method not in {"GET", "PUT", "PATCH", "DELETE"}:
                    # Bad Request
                    await loop.sock_sendall(conn_sock, utils.RESPONSE_400)
                    break  # close connection
                if version == "HTTP/1.0":
                    # HTTP Version not supported
                    await loop.sock_sendall(conn_sock, utils.RESPONSE_505)
                    break  # close connection
                if "Content-Length:" in request and "Content-Length: 0" not in request:
                    # Bad Request, content in requests is not supported
                    await loop.sock_sendall(conn_sock, utils.RESPONSE_400)
                    break  # close connection
                # check and switch to RW db if needed
                if method != "GET" and current_db is context.db_ro:
                    if len(context.db_rw_set_idle):
                        current_db = context.db_rw_set_idle.pop()
                    else:
                        current_db = Database(loop, const.DB_RW_URI)
                context.db_rw_set_active.add(current_db)
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
            if current_db is not context.db_ro:
                if await current_db.in_transaction():
                    await current_db.execute("ROLLBACK")
    # return rw db back to idle pool or just discard it if it is already big enough
    if current_db is not context.db_ro and len(context.db_rw_set_idle) < const.DB_RW_POOL_SIZE:
        context.db_rw_set_idle.add(current_db)
    context.db_rw_set_active.discard(current_db)
    conn_sock.close()
    print(f"handle_connection {addr}: closed")


async def process_request(method: str, req_path: str, db: Database) -> tuple:
    """
    :return: tuple (bytes, bool) - (response, keep connection open)
    """
    url = urllib.parse.urlparse(req_path)
    query = dict(
        (k, (v[0] if isinstance(v, list) and len(v) == 1 else v)) for k, v in urllib.parse.parse_qs(url.query).items())
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
            response = utils.RESPONSE_404, False  # Not Found
    except Exception as ex:
        result_json = json.dumps({"Error": str(ex)}, indent=4)
        response = utils.compose_response(500, result_json + '\n'), False
    return response


async def handle_ping(method: str, query: dict) -> tuple:
    if method != "GET":
        return utils.RESPONSE_400, False  # Bad Request
    await asyncio.sleep(1)
    response = utils.RESPONSE_200, True  # OK
    return response


############################  db path handlers

async def handle_db_begin(method: str, db: Database) -> tuple:
    if method != "PUT":
        return utils.RESPONSE_400, False  # Bad Request
    await db.execute("BEGIN")
    response = utils.RESPONSE_200, True  # OK
    return response


async def handle_db_commit(method: str, db: Database) -> tuple:
    if method != "PUT":
        return utils.RESPONSE_400, False  # Bad Request
    await db.execute("COMMIT")
    response = utils.RESPONSE_200, True  # OK
    return response


async def handle_db_rollback(method: str, db: Database) -> tuple:
    if method != "PUT":
        return utils.RESPONSE_400, False  # Bad Request
    await db.execute("ROLLBACK")
    response = utils.RESPONSE_200, True  # OK
    return response


############################  pii path handlers

def validate_query(query: dict, include_new=False, check_pkey=False) -> tuple:
    """
    :return: empty - means all valid
    """
    query_keys = set(query.keys())
    # validate & encode SSN
    if "SSN" in query_keys:
        if not utils.is_SSN(query["SSN"]):
            result_json = json.dumps({"Error": "Invalid SSN'"})
            return utils.compose_response(400, result_json + '\n'), False  # Err
        query['SSN'] = utils.encode_SSN(query['SSN'])
    # validate DOB
    if "DOB" in query_keys and not utils.is_date(query["DOB"]):
        result_json = json.dumps({"Error": f"Invalid DOB: '{query['DOB']}'"})
        return utils.compose_response(400, result_json + '\n'), False  # Err
    # validate fields are from allowed set
    allowed_keys = set(const.DB_PII_TABLE_FIELDS)
    if include_new:
        new_allowed_keys = set()
        for k in allowed_keys:
            new_allowed_keys.add(k + '_new')
            new_allowed_keys.add(k)
        allowed_keys = new_allowed_keys
    if not set(query_keys).issubset(allowed_keys):
        diff_set = set(const.DB_PII_TABLE_FIELDS) - set(query_keys)
        result_json = json.dumps({"Error": f"Unknown fields {diff_set}." +
                                           f"Allowed: {const.DB_PII_TABLE_FIELDS}"}, indent=4)
        return utils.compose_response(400, result_json + '\n'), False  # Err
    if check_pkey:
        # check that fields are matching rimary kay fields
        if not set(const.DB_PII_TABLE_PKEY).issubset(set(query.keys())):
            diff_set = set(const.DB_PII_TABLE_PKEY) - set(query.keys())
            result_json = json.dumps({"Error": f"Missing fields {diff_set}." +
                                               f"Required: {const.DB_PII_TABLE_PKEY}"}, indent=4)
            return utils.compose_response(400, result_json + '\n'), False  # Err
    return ()


async def handle_pii_search(method: str, query: dict, db: Database) -> tuple:
    if method != "GET":
        return utils.RESPONSE_400, False  # Bad Request
    # validate query fields & vals
    reponse_tuple = validate_query(query)
    if reponse_tuple:
        return reponse_tuple
    # compose sql stmt
    query_keys = set(query.keys())
    where_clause = ' AND '.join([f"{k} = :{k}" for k in query_keys])
    fields = ", ".join(const.DB_PII_TABLE_FIELDS)
    sql = f"""
    SELECT {fields}
    FROM pii_table
    WHERE {where_clause};         
    """
    result = await db.execute(sql, query)
    new_result = utils.normalize_db_result(result)
    result_json = json.dumps(new_result, indent=4)
    response = utils.compose_response(200, result_json + '\n'), True  # OK
    return response


async def handle_pii_insert(method: str, query: dict, db: Database) -> tuple:
    if method != "PUT":
        return utils.RESPONSE_400, False  # Bad Request
    # validate query fields & vals
    reponse_tuple = validate_query(query)
    if reponse_tuple:
        return reponse_tuple
    # compose sql stmt
    values = ":" + ", :".join(const.DB_PII_TABLE_FIELDS)
    fields = ", ".join(const.DB_PII_TABLE_FIELDS)
    sql = f"""
    INSERT INTO  pii_table ({fields})
    VALUES ({values})
    RETURNING {fields};         
    """
    result = await db.execute(sql, query)
    new_result = utils.normalize_db_result(result)
    result_json = json.dumps(new_result, indent=4)
    response = utils.compose_response(200, result_json + '\n'), True  # OK
    return response


async def handle_pii_update(method: str, query: dict, db: Database) -> tuple:
    """
    values to be updated are provided with "_new" name suffix:
        field1=value1&field1_new=value2
    """
    if method != "PATCH":
        return utils.RESPONSE_400, False  # Bad Request
    # validate query fields & vals
    reponse_tuple = validate_query(query, include_new=True, check_pkey=True)
    if reponse_tuple:
        return reponse_tuple
    # extract fields that need to be updated
    query_new = dict()
    query_old = dict()
    for k, v in query.items():
        if k.endswith("_new"):
            query_new[k] = v
        else:
            query_old[k] = v
    # check for non empty query_new
    if not query_new:
        result_json = json.dumps({"Error": f"Missing new values"}, indent=4)
        return utils.compose_response(400, result_json + '\n'), False  # Err
    # compose sql stmt
    fields = ", ".join(const.DB_PII_TABLE_FIELDS)
    where_clause = ' AND '.join([f"{k} = :{k}" for k in query_old.keys()])
    set_clause = ', '.join([f'{k[:-4]} = :{k}' for k in query_new.keys()])
    sql = f"""
    UPDATE pii_table
    SET {set_clause}
    WHERE {where_clause}
    RETURNING {fields};         
    """
    result = await db.execute(sql, {**query_old, **query_new})
    new_result = utils.normalize_db_result(result)
    result_json = json.dumps(new_result, indent=4)
    response = utils.compose_response(200, result_json + '\n'), True  # OK
    return response


async def handle_pii_delete(method: str, query: dict, db: Database) -> tuple:
    if method != "DELETE":
        return utils.RESPONSE_400, False  # Bad Request
    # validate query fields & vals
    reponse_tuple = validate_query(query, check_pkey=True)
    if reponse_tuple:
        return reponse_tuple
    # compose sql stmt
    query_keys = set(query.keys())
    where_clause = ' AND '.join([f"{k} = :{k}" for k in query_keys])
    fields = ", ".join(const.DB_PII_TABLE_FIELDS)
    sql = f"""
    DELETE FROM pii_table
    WHERE {where_clause}
    RETURNING {fields};
    """
    result = await db.execute(sql, query)
    new_result = utils.normalize_db_result(result)
    result_json = json.dumps(new_result, indent=4)
    response = utils.compose_response(200, result_json + '\n'), True  # OK
    return response


#############################################  misc & main

async def refill_rate_limit_buckets(buckets: dict, interval_sec: int, limit: int) -> None:
    while True:
        await asyncio.sleep(interval_sec)
        for key in buckets.keys():
            buckets[key] = limit  # TODO: shrink buckets based on last activity time, extract into class/func


async def shutdown_watchdog(ctx: Context, interval_sec: float):
    while True:
        await asyncio.sleep(interval_sec)
        global g_shutdown
        if g_shutdown:
            ctx.server_socket.close()
            for db in ctx.db_rw_set_idle:
                db.shutdown()
            for db in ctx.db_rw_set_active:
                db.shutdown()
            break


def shutdown():
    print('Got a SIGINT!')
    global g_shutdown
    g_shutdown = True
    time.sleep(1)
    tasks = asyncio.all_tasks()
    print(f'Cancelling {len(tasks)} task(s).')
    [task.cancel() for task in tasks]
    print(f'Active tasks: {len(asyncio.all_tasks())}')


def main():
    parser = argparse.ArgumentParser(description=f"{APP_NAME} v{__version__}")
    parser.parse_args()
    #
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    loop.add_signal_handler(signal.SIGHUP, functools.partial(shutdown, loop))
    loop.add_signal_handler(signal.SIGTERM, functools.partial(shutdown, loop))
    #
    try:
        loop.create_task(serve(loop))
        loop.run_forever()
    finally:
        loop.close()
    print("Finished")


if __name__ == "__main__":
    main()

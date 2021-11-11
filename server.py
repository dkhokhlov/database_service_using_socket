import socket
import argparse
import asyncio
import signal
import urllib.parse
import traceback
import database

import constants as const


class Context:
    def __init__(self, loop, db_ro, db_rw_pool, rate_limit_buckets):
        self.db_ro = db_ro
        self.db_rw_pool = asyncio.Queue()
        self.loop = loop
        self.rate_limit_buckets = rate_limit_buckets


async def serve(loop):
    """
    """
    rate_limit_buckets = {}
    loop.create_task(refill_rate_limit_buckets(rate_limit_buckets, const.RATE_LIMIT_NUM, const.RATE_LIMIT_SEC))
    # create ro db used by GETs
    db_ro = database.Database(loop, const.DB_RO_URI)
    # rw db pool used by mutable methods, unbound grow with low limited shrink
    db_rw_pool = asyncio.Queue()  # unbound on get, but manually limited to const.db_rw_pool_SIZE on put
    context = Context(loop, db_ro, db_rw_pool, rate_limit_buckets)
    # setup ipv4 TCP socket server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
        s.listen(const.BACKLOG)
        s.setblocking(False)

        while True:
            conn, addr = await loop.sock_accept(s)
            print("Connected by", addr)
            conn.setblocking(False)
            loop.create_task(handle_connection(conn, addr, context))


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


async def process_request(request: str) -> tuple:
    """
    :param request:
    :return: tuple (bytes, bool) - response, keep connection open
    """
    lines = request.split("\n")
    method, req_path, version = lines[0].split(" ")
    if method not in {"GET", "PUT", "PATCH", "DELETE"}:
        response = compose_response(400), False  # Bad Request
        return response
    url = urllib.parse.urlparse(req_path)
    query = urllib.parse.parse_qs(url.query)
    path = url.path
    if version == "HTTP/1.0":
        response = compose_response(505), False  # HTTP Version not supported
        return response,
    if "content-length:" in request.lower():
        response = compose_response(400), False  # Bad Request
        return response
    if path == "/ping":
        await delay()
        response = compose_response(200), True  # OK
    else:
        response = compose_response(404), False  # Not Found
    return response


async def refill_rate_limit_buckets(buckets: dict, interval_sec: int, limit: int) -> None:
    while True:
        await asyncio.sleep(interval_sec)
        for key in buckets.keys():
            buckets[key] = limit  # TODO: shrinking based on last activity time, extract


async def handle_connection(conn_sock, addr, context: Context) -> None:
    print(f"handle_connection {addr}: begin")
    rate_limit_buckets = context.rate_limit_buckets
    loop = context.loop
    with conn_sock:
        try:
            tokens = rate_limit_buckets.get(addr[0], const.RATE_LIMIT_NUM)
            if tokens > 0:
                rate_limit_buckets[addr[0]] = tokens - 1
            else:
                response = compose_response(429)  # Too many requests
                await loop.sock_sendall(conn_sock, response)
                return
            buffer = ''
            while True:
                data = await loop.sock_recv(conn_sock, 2048)  # assume all requests are smaller than 2048 bytes
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
                # process request
                response, keep_open = await process_request(request)
                print(f"Response: \n{response.decode('utf-8')}")
                # send response
                await loop.sock_sendall(conn_sock, response)
                if not keep_open:
                    break
        except Exception:
            print(traceback.format_exc())
    print(f"handle_connection {addr}: closed")


async def delay():
    await asyncio.sleep(1)


def shutdown():
    print('Got a SIGINT!')
    tasks = asyncio.all_tasks()
    print(f'Cancelling {len(tasks)} task(s).')
    [task.cancel() for task in tasks]


def main():
    parser = argparse.ArgumentParser(description="PII microservice")
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


if __name__ == "__main__":
    main()

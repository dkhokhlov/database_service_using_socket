import socket
import time
import argparse
import asyncio
import signal

import constants as const


async def serve():
    """
    Note: you should implement http methods by hand, using a socket server
    as below (or similar to below).

    Do not use an http server from the python standard library.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # enable TCP socket keep-alive
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Linux specific
        assert (hasattr(socket, "TCP_KEEPIDLE") and hasattr(socket, "TCP_KEEPINTVL") and hasattr(socket, "TCP_KEEPCNT"))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, const.TCP_KEEPIDLE_SEC)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, const.TCP_KEEPINTVL_SEC)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, const.TCP_KEEPCNT_SEC)

        print(f"Listening on {const.HOST}:{const.PORT}")

        s.bind((const.HOST, const.PORT))
        s.listen(const.BACKLOG)
        s.setblocking(False)

        loop = asyncio.get_event_loop()

        while True:
            conn, addr = await loop.sock_accept(s)
            print("Connected by", addr)
            conn.setblocking(False)
            loop.create_task(handle_connection(conn, addr, loop))


async def handle_connection(conn_sock, addr, loop):
    print(f"handle_connection[{conn_sock.fileno()}]: begin", addr)
    with conn_sock:
        while True:
            data = await loop.sock_recv(conn_sock, 2048)  # assume all requests are smaller than 2048 bytes
            raw = data.decode("utf-8")
            lines = raw.split("\r\n")
            request = "  \n".join(lines)
            print(f"Request: \n{request}")

            method, path, version = lines[0].split(" ")
            if path == "/ping":
                await delay()
                response = "HTTP/1.1 200 Ok\n\n"
            else:
                response = "xxx"


            print(f"Response: \n{response}")
            await loop.sock_sendall(conn_sock, response.encode("utf-8"))
            print(f"handle_connection[{conn_sock.fileno()}]: end", addr)
            break


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
        loop.create_task(serve())
        loop.run_forever()
    finally:
        loop.close()
    print("Finished")


if __name__ == "__main__":
    main()

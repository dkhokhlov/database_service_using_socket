import socket
import time
import argparse
import constants as const


def serve():
    """
    Note: you should implement http methods by hand, using a socket server
    as below (or similar to below).

    Do not use an http server from the python standard library.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 10)
        # Linux specific
        assert (hasattr(socket, "TCP_KEEPIDLE") and hasattr(socket, "TCP_KEEPINTVL") and hasattr(socket, "TCP_KEEPCNT"))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, const.TCP_KEEPIDLE_SEC)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, const.TCP_KEEPINTVL_SEC)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, const.TCP_KEEPCNT_SEC)

        print(f"Listening on {const.HOST}:{const.PORT}")

        s.bind((const.HOST, const.PORT))
        s.listen(const.BACKLOG)

        while True:
            conn, addr = s.accept()
            with conn:
                print("Connected by", addr)
                while True:
                    data = conn.recv(2048)  # assume all requests are smaller than 2048 bytes
                    raw = data.decode("utf-8")
                    lines = raw.split("\r\n")
                    request = "  \n".join(lines)
                    print(f"Request: \n{request}")

                    method, path, version = lines[0].split(" ")
                    if path == "/ping":
                        delay()
                        response = "HTTP/1.1 200 Ok\n\n"
                    else:
                        response = "xxx"

                    delay()

                    print(f"Response: \n{response}")
                    conn.sendall(response.encode("utf-8"))
                    break


def delay():
    time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="PII microservice")
    parser.parse_args()
    serve()


if __name__ == "__main__":
    main()

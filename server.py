import socket
import time

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)


def serve():
    """
    Note: you should implement http methods by hand, using a socket server
    as below (or similar to below).

    Do not use an http server from the python standard library.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        print(f"Listening on {HOST}:{PORT}")
        s.bind((HOST, PORT))
        s.listen()

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
    serve()


if __name__ == "__main__":
    main()

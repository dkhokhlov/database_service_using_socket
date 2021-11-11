from http import client
import asyncio
import constants
import time
from concurrent.futures import ThreadPoolExecutor


def ping(index: int) -> int:
    print(f"{index}: ping")
    c = client.HTTPConnection(constants.HOST, constants.PORT)
    c.request("GET", "/ping")
    response = c.getresponse().getcode()
    print(f"f{index}: response: {response}")
    return response


async def load_test(num_tasks: int):
    executor = ThreadPoolExecutor(num_tasks)
    start = time.time()
    tasks = [executor.submit(ping, i) for i in range(num_tasks)]

    results = [task.result() for task in tasks]
    elapsed = time.time() - start

    print(f"Results: {results}, ")

    assert elapsed < (
        num_tasks / 2
    ), f"FAIL: Load test took {elapsed:.2f} seconds. Make your server faster/concurrent."


def main():
    asyncio.run(load_test(3))


if __name__ == "__main__":
    main()

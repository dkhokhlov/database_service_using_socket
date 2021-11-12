import traceback
from http import client
import constants
import urllib
import json


def catch(func):
    def handler(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as ex:
            print(f"FAIL: exceptional response: {ex}")
            traceback.print_exc()

    return handler


@catch
def test(
        name: str,
        path: str,
        method: str = "GET",
        body: str = "",
        expected_response=200,
        expect_body: bool = False,
        expect_fields: dict = None,
):
    print(f"Running Test {name}")
    c = client.HTTPConnection(constants.HOST, constants.PORT)
    c.request(method, path, body=body)
    response = c.getresponse()
    response_body = response.read()
    print(f"Response: {response.getcode()}\n{response_body}")

    assert response.getcode() == expected_response
    assert (response_body and expect_body) or (not response_body and not expect_body)

    if expect_fields:
        # parse json to dict and look for key/value
        ...
    print(f"Success({name})!\n")


@catch
def test_ping():
    test("PingTest", "/ping")


@catch
def test_single_insert():
    print("InsertTest")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_enc = urllib.parse.urlencode(query)
        test("InsertTest", path=f"/pii/insert?{query_enc}", method="PUT", expected_response=400, expect_body=True)


def main():
    tests = [
        test_ping,
        test_single_insert,
    ]
    for test in tests:
        test()


if __name__ == "__main__":
    main()

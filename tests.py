import traceback
from http import client
import constants
import urllib
import json
import utils


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
        if "SSN" in expect_fields.keys():
            expect_fields['SSN'] = utils.encode_SSN(expect_fields['SSN'])
        json_obj = json.loads(response_body)
        for k in expect_fields.keys():
            assert(json_obj[0][k] == expect_fields[k])

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
        test("InsertTest", path=f"/pii/insert?{query_enc}", method="PUT",
             expected_response=200, expect_body=True, expect_fields=query)

@catch
def test_single_delete():
    print("InsertTest")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_enc = urllib.parse.urlencode(query)
        test("DeleteTest", path=f"/pii/delete?{query_enc}", method="DELETE",
             expected_response=200, expect_body=True, expect_fields=query)


def main():
    tests = [
        test_ping,
        test_single_insert,
        test_single_delete,
    ]
    for test in tests:
        test()


if __name__ == "__main__":
    main()

import traceback
from http import client
import constants
import urllib
import json
import utils
import time


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
        connection:client.HTTPConnection = None
):
    print(f"Running Test {name}")
    if not connection:
        c = client.HTTPConnection(constants.HOST, constants.PORT)
    else:
        c = connection
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
        if expect_fields:
            for k in expect_fields.keys():
                assert (json_obj[0][k] == expect_fields[k])
        else:
            if expect_fields is not None:
                assert (len(json_obj) == 0)
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
        query_url = urllib.parse.urlencode(query)
        test("InsertTest", path=f"/pii/insert?{query_url}", method="PUT",
             expected_response=200, expect_body=True, expect_fields=query)


@catch
def test_single_delete():
    print("DeleteTest")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_url = urllib.parse.urlencode(query)
        test("DeleteTest", path=f"/pii/delete?{query_url}", method="DELETE",
             expected_response=200, expect_body=True, expect_fields=query)


@catch
def test_single_delete_no_fields():
    print("DeleteTest no fields")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_url = urllib.parse.urlencode(query)
        test("DeleteTest no fields", path=f"/pii/delete?{query_url}", method="DELETE", expect_body=True,
             expected_response=200)


@catch
def test_delay():
    time.sleep(0.5)


@catch
def test_single_update():
    print("UpdateTest")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_new = query.copy()
        query_new["first_name_new"] = "Bill2"
        query_expected = query.copy()
        query_expected["first_name"] = "Bill2"
        query_url = urllib.parse.urlencode(query_expected)
        test("DeleteTest", path=f"/pii/delete?{query_url}", method="DELETE",
             expected_response=200, expect_body=True)
        query_url = urllib.parse.urlencode(query_new)
        test("UpdateTest", path=f"/pii/update?{query_url}", method="PATCH",
             expected_response=200, expect_body=True, expect_fields=query_expected)


@catch
def test_single_search():
    print("SearchTest")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_url = urllib.parse.urlencode(query)
        test("SearchTest", path=f"/pii/search?{query_url}", method="GET",
             expected_response=200, expect_body=True, expect_fields=query)


@catch
def test_single_rollback():
    print("RollbackTest")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_url = urllib.parse.urlencode(query)
        # nwihtout reusing connection - rollback must fail
        test("RollbackTest1", path=f"/db/begin", method="PUT",
             expected_response=200, expect_body=False, expect_fields=None)
        test("RollbackTest2", path=f"/db/rollback", method="PUT",
             expected_response=500, expect_body=True, expect_fields=None)
        time.sleep(0.5)
        # now reusing connection
        c = client.HTTPConnection(constants.HOST, constants.PORT)
        test("RollbackTest3", path=f"/db/begin", method="PUT",
             expected_response=200, expect_body=False, expect_fields=None, connection=c)
        time.sleep(0.5)
        test("RollbackTest4", path=f"/db/rollback", method="PUT",
             expected_response=200, expect_body=False, expect_fields=None, connection=c)


@catch
def test_single_update_with_rollback():
    print("UpdateTest_with_rollback")
    with open("data/pb.json", "r") as f:
        json_str = f.read()
        query = json.loads(json_str)
        query_new = query.copy()
        query_new["first_name_new"] = "Bill2"
        query_expected = query.copy()
        query_expected["first_name"] = "Bill2"
        query_expected2 = query_expected.copy()
        query_url = urllib.parse.urlencode(query_expected)
        # now updating with reusing connection and begin + rollback
        c = client.HTTPConnection(constants.HOST, constants.PORT)  # need to reuse connection
        test("UpdateTest_with_rollback1", path=f"/pii/delete?{query_url}", method="DELETE",
             expected_response=200, expect_body=True, connection=c)
        time.sleep(0.5)
        test("UpdateTest_with_rollback2", path=f"/db/begin", method="PUT",
             expected_response=200, expect_body=False, expect_fields=None, connection=c)
        query_url = urllib.parse.urlencode(query_new)
        time.sleep(0.5)
        test("UpdateTest_with_rollback2", path=f"/pii/update?{query_url}", method="PATCH",
             expected_response=200, expect_body=True, expect_fields=query_expected, connection=c)
        test("UpdateTest_with_rollback3", path=f"/db/rollback", method="PUT",
             expected_response=200, expect_body=False, expect_fields=None, connection=c)
        # now we run new session with update only - it should not see last update (no PKEY violation)
        time.sleep(0.5)
        query_url = urllib.parse.urlencode(query_expected2)
        test("RollbackTest6", path=f"/pii/search?{query_url}", method="GET",
             expected_response=200, expect_body=True, expect_fields={})

def main():
    tests = [
        test_ping,
        test_single_delete_no_fields,
        test_delay,# to avoid rate limiter
        test_single_insert,
        test_single_delete,
        test_delay,  # to avoid rate limiter
        test_single_insert,
        test_single_search,
        test_delay,  # to avoid rate limiter
        test_single_update,
        test_delay,  # to avoid rate limiter
        test_single_rollback,
        test_delay,  # to avoid rate limiter
        test_single_insert,
        test_single_update_with_rollback
    ]
    for test in tests:
        test()


if __name__ == "__main__":
    main()

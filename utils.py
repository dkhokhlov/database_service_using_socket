import hashlib
import re
import datetime as dt
import constants as const
from collections import OrderedDict

reg_pat_ssn = re.compile(r'^(?!000|.+0{4})(?:\d{9}|\d{3}-\d{2}-\d{4})$')


def is_date(date_str: str) -> bool:
    try:
        dt.datetime.strptime(date_str, '%m-%d-%Y')
        return True
    except ValueError:
        return False


def is_SSN(ssn_str: str) -> bool:
    m = reg_pat_ssn.match(ssn_str)
    return bool(m)


def encode_SSN(ssn: str) -> str:
    norm_ssn = ssn.replace("-", "").replace(" ", "")
    return hashlib.blake2b(norm_ssn.encode('utf8'), salt=const.HMAC_SALT).hexdigest()


def normalize_db_result(result: list) -> list:
    """
    :return: list of dictionaries
    """
    new_result = []
    for i, rec in enumerate(result):
        new_rec = OrderedDict()
        for k, v in zip(const.DB_PII_TABLE_FIELDS, rec):
            new_rec[k] = v
        new_result.append(new_rec)
    return new_result


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


# static HTTP responses
RESPONSE_200 = compose_response(200)  # OK
RESPONSE_400 = compose_response(400)  # Bad Request
RESPONSE_404 = compose_response(404)  # Not Found
RESPONSE_429 = compose_response(429)  # Too many requests sent
RESPONSE_500 = compose_response(500)  # Internal Server Error
RESPONSE_503 = compose_response(503)  # Busy
RESPONSE_505 = compose_response(505)  # HTTP Version Not Supported

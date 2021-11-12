import hashlib
import re
import datetime as dt
import constants as const

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

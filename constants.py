HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)

TCP_LISTEN_BACKLOG = 30 # accept backlog

HTTP_BUFFER_SIZE = 2048

# TCP Keep-Alive for fast disconnect detection
# after 5 sec, start sending keepalives every 5 sec,
# drop connection after 1 failed keepalive
TCP_KEEPIDLE_SEC = 10
TCP_KEEPINTVL_SEC = 10
TCP_KEEPCNT_SEC = 1

# Database URIs for different modes for the same DB
#DB_FILE = ':memory:'
DB_FILE = '/dev/shm/pii_service.db'  # needs to be copied to non-volatile storage after service shutdown
DB_RO_URI = f'file:{DB_FILE}?cache=shared&mode=ro'
DB_RW_URI = f'file:{DB_FILE}?cache=shared'
DB_RW_POOL_SIZE = 10000

DB_DDL_SQL = """
CREATE TABLE IF NOT EXISTS  pii_table(
    first_name TEXT,
    last_name TEXT,
    occupation TEXT,
    SSN TEXT,
    DOB DATE,
    PRIMARY KEY(first_name, last_name, occupation, SSN, DOB)
);
"""

# Rate limiter
RATE_LIMIT_SEC = 1
RATE_LIMIT_NUM = 5



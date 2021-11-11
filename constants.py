HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)
BACKLOG = 30 # accept backlog

# after 10 sec, start sending keepalives every 10 sec,
# drop connection after 1 failed keepalive
TCP_KEEPIDLE_SEC = 10
TCP_KEEPINTVL_SEC = 10
TCP_KEEPCNT_SEC = 1

# Database
DB_URI = 'file::memory:?cache=shared'

# Rate limiter
RATE_LIMIT_SEC = 1
RATE_LIMIT_NUM = 5


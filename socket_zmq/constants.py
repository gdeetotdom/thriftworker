"""All constants for this package."""
from struct import calcsize
import errno


DEFAULT_ENV = 0x1
GEVENT_ENV = 0x2

STATUS_FORMAT = '!?'
LENGTH_FORMAT = '!i'

LENGTH_SIZE = calcsize(LENGTH_FORMAT)
BUFFER_SIZE = 4096

RCVTIMEO = 100

BACKLOG_SIZE = 1024
POOL_SIZE = 1024

NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

"""All constants for this package."""
from __future__ import absolute_import

import errno
from struct import calcsize


DEFAULT_ENV = 0x1
GEVENT_ENV = 0x2

LENGTH_FORMAT = '!i'

LENGTH_SIZE = calcsize(LENGTH_FORMAT)

BACKLOG_SIZE = 1024

NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

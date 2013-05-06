"""All constants for this package."""
from __future__ import absolute_import

import errno
from struct import calcsize


LENGTH_FORMAT = '!i'

LENGTH_SIZE = calcsize(LENGTH_FORMAT)

BACKLOG_SIZE = 1024

NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

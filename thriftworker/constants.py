"""All constants for this package."""
from __future__ import absolute_import

import errno
from struct import calcsize


DEFAULT_ENV = 0x1
GEVENT_ENV = 0x2

ENVS = {
    DEFAULT_ENV: 'thriftworker.envs.sync:SyncEnv',
    GEVENT_ENV: 'thriftworker.envs.green:GeventEnv',
}

WORKERS = {
    DEFAULT_ENV: 'thriftworker.workers.sync:SyncWorker',
    GEVENT_ENV: 'thriftworker.workers.green:GeventWorker',
}

LENGTH_FORMAT = '!i'

LENGTH_SIZE = calcsize(LENGTH_FORMAT)

BACKLOG_SIZE = 1024

NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

import _socket
import logging
import errno

import cython
from pyev import EV_READ, EV_MINPRI, Io

from .constants import BACKLOG_SIZE, NONBLOCKING
from .pool cimport SinkPool
from .source cimport SocketSource

__all__ = ['Proxy']

logger = logging.getLogger(__name__)


cdef class Proxy(object):

    def __init__(self, object loop, object name, object socket,
                 SinkPool pool, object backlog=None):
        self.connections = set()
        self.loop = loop
        self.name = name
        self.socket = socket._sock
        self.pool = pool
        self.backlog = backlog or BACKLOG_SIZE
        self.watcher = Io(self.socket, EV_READ, self.loop,
                          self.on_connection, priority=EV_MINPRI)

    def on_connection(self, object watcher, object revents):
        while True:
            try:
                result = self.socket.accept()
            except _socket.error as exc:
                if exc[0] in NONBLOCKING:
                    return
                elif exc[0] == errno.EMFILE:
                    logger.exception(exc)
                    return
                raise
            client_socket = result[0]
            client_socket.setblocking(0)
            # Disable the Nagle algorithm.
            client_socket.setsockopt(_socket.SOL_TCP, _socket.TCP_NODELAY, 1)
            # Set TOS to IPTOS_LOWDELAY.
            client_socket.setsockopt(_socket.IPPROTO_IP, _socket.IP_TOS, 0x10)
            self.connections.add(SocketSource(self.name, self.pool, self.loop,
                                              client_socket, self.on_close))

    def on_close(self, SocketSource source):
        try:
            self.connections.remove(source)
        except KeyError:
            pass

    def start(self):
        self.socket.listen(self.backlog)
        self.watcher.start()

    def stop(self):
        self.socket.close()
        self.watcher.stop()
        while self.connections:
            connection = self.connections.pop()
            if not connection.is_closed():
                connection.close()

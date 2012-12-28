from __future__ import absolute_import

import errno
import logging
from contextlib import contextmanager
from collections import deque
from abc import ABCMeta, abstractproperty

from pyuv import Async, Pipe, Poll, UV_READABLE
from pyuv.errno import strerror
from six import with_metaclass

from thriftworker.constants import BACKLOG_SIZE
from thriftworker.utils.mixin import LoopMixin
from thriftworker.utils.loop import in_loop
from thriftworker.utils.decorators import cached_property

logger = logging.getLogger(__name__)


@contextmanager
def ignore_eagain(socket):
    """Ignore all *EAGAIN* errors in context."""
    try:
        yield
    except socket.error as exc:
        if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EINVAL):
            raise


class Connections(object):
    """Store connections."""

    def __init__(self):
        self.connections = set()

    def __len__(self):
        return len(self.connections)

    def register(self, connection):
        """Register new connection."""
        self.connections.add(connection)

    def remove(self, connection):
        """Remove registered connection."""
        try:
            self.connections.remove(connection)
        except KeyError:
            logger.warning('Connection %r not registered', connection)

    def close(self):
        connections = self.connections
        while connections:
            connection = connections.pop()
            if not connection.is_closed():
                connection.close()


class BaseAcceptor(with_metaclass(ABCMeta, LoopMixin)):

    Connections = Connections

    def __init__(self, name, descriptor, backlog=None):
        self.name = name
        self.descriptor = descriptor
        self.backlog = backlog or BACKLOG_SIZE
        self._connections = self.Connections()
        super(BaseAcceptor, self).__init__()

    @cached_property
    def _poller(self):
        return Poll(self.loop, self.descriptor)

    @cached_property
    def _socket(self):
        socket = self.app.env.socket
        sock = socket.fromfd(self.descriptor, socket.AF_INET,
                             socket.SOCK_STREAM)
        sock.setblocking(0)
        return sock

    @abstractproperty
    def Connection(self):
        """Return connection class. Depends on current
        implementation of transport.

        """
        raise NotImplementedError()

    @property
    def connections_number(self):
        """Return number of active connections."""
        return len(self._connections)

    @property
    def active(self):
        """Is current acceptor active."""
        return self._poller.active

    def create_acceptor(self):
        """Return function that should accept new connections."""
        loop = self.loop
        service = self.name
        connections = self._connections
        producer = self.app.worker.create_producer(service)
        socket = self.app.env.socket
        listen_sock = self._socket

        def on_close(connection):
            """Callback called when connection closed."""
            connections.remove(connection)

        def accept_connection(handle, events, error):
            """Function that try to accept new connection."""
            if error:  # pragma: no cover
                logger.error('Error handling new connection for'
                             ' service %r: %s', service, strerror(error))
                return
            with ignore_eagain(socket):
                sock, addr = listen_sock.accept()
                # Disable Nagle's algorithm for socket.
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client = Pipe(loop)
                client.open(sock.fileno())
                connection = self.Connection(producer, loop, client, sock,
                                             on_close)
                connections.register(connection)

        return accept_connection

    @in_loop
    def start(self):
        if not self.active:
            self._poller.start(UV_READABLE, self.create_acceptor())

    @in_loop
    def stop(self):
        if self.active:
            self._poller.stop()

    @in_loop
    def close(self):
        """Close all resources."""
        self._poller.close()
        self._connections.close()
        self._socket.close()


class Acceptors(LoopMixin):
    """Maintain pool of acceptors. Start them when needed."""

    def __init__(self):
        self._outgoing = deque()
        self._acceptors = {}
        super(Acceptors, self).__init__()

    def __iter__(self):
        """Iterate over registered acceptors."""
        return iter(self._acceptors.values())

    @cached_property
    def Acceptor(self):
        """Shortcut to :class:`thriftworker.acceptor.Acceptor` class."""
        return self.app.Acceptor

    @cached_property
    def _handle(self):
        """Handle that should start acceptors in loop thread."""
        outgoing = self._outgoing

        def cb(handle):
            while True:
                try:
                    callback = outgoing.popleft()
                except IndexError:
                    break
                else:
                    callback()

        return Async(self.loop, cb)

    def register(self, fd, name, backlog=None):
        """Register new acceptor in pool."""
        self._acceptors[name] = self.Acceptor(name, fd, backlog=backlog)

    def start_by_name(self, name):
        acceptor = self._acceptors[name]
        self._outgoing.append(acceptor.start)
        self._handle.send()

    def stop_by_name(self, name):
        acceptor = self._acceptors[name]
        self._outgoing.append(acceptor.stop)
        self._handle.send()

    @in_loop
    def start(self):
        self._handle.send()

    @in_loop
    def stop(self):
        self._handle.close()
        for acceptor in self._acceptors.values():
            acceptor.close()

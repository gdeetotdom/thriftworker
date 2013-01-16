from __future__ import absolute_import

import socket
import errno
import logging
from contextlib import contextmanager
from abc import ABCMeta, abstractproperty

from pyuv import Pipe, Poll, UV_READABLE
from pyuv.errno import strerror
from six import with_metaclass

from thriftworker.constants import BACKLOG_SIZE
from thriftworker.utils.mixin import LoopMixin, StartStopMixin
from thriftworker.utils.loop import in_loop
from thriftworker.utils.decorators import cached_property
from thriftworker.utils.waiting import wait

logger = logging.getLogger(__name__)


@contextmanager
def ignore_eagain():
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

    def __iter__(self):
        return iter(self.connections)

    def __repr__(self):
        return repr(self.connections)

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
                logger.warn('Connection %r closed in worker shutdown',
                            connection)
                connection.close()


class BaseAcceptor(with_metaclass(ABCMeta, LoopMixin)):
    """Accept incoming connections."""

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
        """Create socket from given descriptor."""
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

    @cached_property
    def acceptor(self):
        """Return function that should accept new connections."""
        loop = self.loop
        service = self.name
        connections = self._connections
        listen_sock = self._socket
        worker = self.app.worker
        producer = worker.create_producer(service)
        concurrency = worker.concurrency

        def on_close(connection):
            """Callback called when connection closed."""
            connections.remove(connection)

        def inner_acceptor(handle, events, error):
            """Function that try to accept new connection."""
            if error:  # pragma: no cover
                logger.error('Error handling new connection for'
                             ' service %r: %s', service, strerror(error))
                return
            if concurrency.reached:
                return
            with ignore_eagain():
                sock, addr = listen_sock.accept()
                # Disable Nagle's algorithm for socket.
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client = Pipe(loop)
                client.open(sock.fileno())
                connection = self.Connection(producer, loop, client, sock,
                                             on_close)
                connections.register(connection)

        return inner_acceptor

    @in_loop
    def start(self):
        """Start acceptor if active."""
        poller = self._poller
        if not poller.active and not poller.closed:
            poller.start(UV_READABLE, self.acceptor)

    @in_loop
    def stop(self):
        """Stop acceptor if active."""
        poller = self._poller
        if poller.active and not poller.closed:
            poller.stop()

    @in_loop
    def close(self):
        """Close all resources."""
        self._poller.close()
        self._connections.close()
        self._socket.close()


class Acceptors(StartStopMixin, LoopMixin):
    """Maintain pool of acceptors. Start them when needed."""

    def __init__(self):
        self._acceptors = {}
        super(Acceptors, self).__init__()

    def __iter__(self):
        """Iterate over registered acceptors."""
        return iter(self._acceptors.values())

    @cached_property
    def Acceptor(self):
        """Shortcut to :class:`thriftworker.acceptor.Acceptor` class."""
        return self.app.Acceptor

    def register(self, fd, name, backlog=None):
        """Register new acceptor in pool."""
        self._acceptors[name] = self.Acceptor(name, fd, backlog=backlog)

    def start_by_name(self, name):
        """Start acceptor by name."""
        acceptor = self._acceptors[name]
        self.app.hub.callback(acceptor.start)

    def stop_by_name(self, name):
        """Stop acceptor by name."""
        acceptor = self._acceptors[name]
        self.app.hub.callback(acceptor.stop)

    def start_accepting(self):
        """Start all registered acceptors if needed."""
        for acceptor in self._acceptors.values():
            acceptor.start()

    def stop_accepting(self):
        """Stop all registered acceptors if needed."""
        for acceptor in self._acceptors.values():
            acceptor.stop()

    def stop(self):
        """Close all registered acceptors."""
        self.stop_accepting()
        # wait for unclosed connections
        wait(lambda: all(acceptor.connections_number == 0
                         for acceptor in self._acceptors.values()),
             timeout=5.0)
        for acceptor in self._acceptors.values():
            acceptor.close()

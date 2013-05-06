from __future__ import absolute_import

import os
import socket
import errno
import logging
from contextlib import contextmanager
from abc import ABCMeta, abstractproperty

from pyuv import TCP, Poll, UV_READABLE
from pyuv.errno import strerror
from six import with_metaclass

from thriftworker.constants import BACKLOG_SIZE
from thriftworker.utils.mixin import LoopMixin, StartStopMixin
from thriftworker.utils.loop import in_loop
from thriftworker.utils.decorators import cached_property
from thriftworker.utils.waiter import Waiter

from . import utils

logger = logging.getLogger(__name__)

NOTBLOCK = (errno.EAGAIN, errno.EWOULDBLOCK, errno.EINVAL, errno.EBADF)


@contextmanager
def ignore_eagain():
    """Ignore all *EAGAIN* errors in context."""
    try:
        yield
    except OSError as exc:
        if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK,
                             errno.EINVAL, errno.EBADF):
            raise


class Connections(object):
    """Store existed connections."""

    def __init__(self):
        self.connections = set()
        self._callback = None

    def __len__(self):
        return len(self.connections)

    def __iter__(self):
        return iter(self.connections)

    def __repr__(self):
        return repr(self.connections)

    def _execute_callback(self):
        """Execute callback if needed."""
        if self._callback is not None:
            self._callback()
            self._callback = None

    @property
    def callback(self):
        """Return callback if existed."""
        return self._callback

    @callback.setter
    def callback(self, cb):
        """Setup callback and execute it if needed."""
        self._callback = cb
        if not self.connections:
            self._execute_callback()

    def register(self, connection):
        """Register new connection."""
        self.connections.add(connection)

    def remove(self, connection):
        """Remove registered connection."""
        try:
            self.connections.remove(connection)
        except KeyError:
            pass
        if not self.connections:
            self._execute_callback()

    def close(self):
        connections = self.connections
        while connections:
            connection = connections.pop()
            if not connection.is_closed():
                logger.warn('Connection %r closed prematurely', connection)
                connection.close()
        self._execute_callback()


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
        sock = socket.fromfd(self.descriptor, socket.AF_INET, socket.SOCK_STREAM)
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
    def empty(self):
        """Is acceptor empty or not."""
        return self.connections_number == 0

    @property
    def active(self):
        """Is current acceptor active."""
        return self._poller.active

    def __iter__(self):
        """Return all registered connections."""
        return iter(self._connections)

    @cached_property
    def acceptor(self):
        """Return function that should accept new connections."""
        loop = self.loop
        service = self.name
        connections = self._connections
        listen_fd = self._socket.fileno()
        worker = self.app.worker
        producer = worker.create_producer(service)

        def on_close(connection):
            """Callback called when connection closed."""
            connections.remove(connection)

        def inner_acceptor(handle, events, error):
            """Function that try to accept new connection."""
            if error:  # pragma: no cover
                logger.error('Error handling new connection for'
                             ' service %r: %s', service, strerror(error))
                return
            try:
                fd, addr = utils.accept_connection(listen_fd)
            except OSError as exc:
                if exc.errno not in NOTBLOCK:
                    raise
                return
            try:
                # Setup socket.
                utils.set_nonblocking(fd)
                utils.set_sockopt(fd, socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except:
                os.close(fd)
                raise
            handle = TCP(loop)
            handle.open(fd)
            connection = self.Connection(producer, loop, handle, addr, on_close)
            connections.register(connection)

        return inner_acceptor

    @in_loop
    def start(self):
        """Start acceptor if active."""
        poller = self._poller
        if not poller.active and not poller.closed:
            poller.start(UV_READABLE, self.acceptor)

    @in_loop
    def stop(self, callback=None):
        """Stop acceptor if active."""
        poller = self._poller
        if poller.active and not poller.closed:
            poller.stop()
        self._connections.callback = callback

    @in_loop
    def close(self):
        """Close all resources."""
        if not self._poller.closed:
            self._poller.close()
        self._connections.close()
        self._socket.close()


class Acceptors(StartStopMixin, LoopMixin):
    """Maintain pool of acceptors. Start them when needed."""

    def __init__(self):
        self._acceptors = {}
        self._stop_waiter = Waiter(
            timeout=self.app.shutdown_timeout)
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

    def stop_accepting(self, callback=None):
        """Stop all registered acceptors if needed."""
        for acceptor in self._acceptors.values():
            acceptor.stop(callback)

    @property
    def connections_number(self):
        """Return current connection number across all acceptors."""
        return sum(acceptor.connections_number for acceptor in self)

    @property
    def empty(self):
        """Are all acceptors empty or not."""
        return self.connections_number == 0

    def stop(self):
        """Close all registered acceptors."""
        # wait for unclosed connections
        def on_close():
            if self.empty:
                self._stop_waiter.done()
        # stop accepting new connection
        self.stop_accepting(callback=on_close)
        # wait for unclosed connections
        if not self.empty:
            logger.info('Waiting for unclosed connections...')
        self._stop_waiter.wait()
        if not self.empty:
            logger.warning('Not all connection closed!')
        # close existed connection
        for acceptor in self:
            acceptor.close()

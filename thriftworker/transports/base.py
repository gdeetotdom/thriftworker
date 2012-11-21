from __future__ import absolute_import

import os
import errno
import logging
from contextlib import contextmanager
from collections import deque
from abc import ABCMeta, abstractproperty

from pyuv import TCP, Async, Pipe, Poll, UV_READABLE
from pyuv.errno import strerror

from thriftworker.constants import BACKLOG_SIZE
from thriftworker.utils.mixin import LoopMixin
from thriftworker.utils.loop import in_loop
from thriftworker.utils.decorators import cached_property

logger = logging.getLogger(__name__)


class Connections(object):
    """Store connections."""

    def __init__(self):
        self.connections = set()

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
        while self.connections:
            connection = self.connections.pop()
            if not connection.is_closed():
                connection.close()


class BaseAcceptor(LoopMixin):

    __metaclass__ = ABCMeta

    Connections = Connections

    def __init__(self, name, descriptor, backlog=None,
                 mutex=None):
        self.name = name
        self.descriptor = descriptor
        self.mutex = mutex
        self.backlog = backlog or BACKLOG_SIZE
        self._connections = self.Connections()
        super(BaseAcceptor, self).__init__()

    @cached_property
    def _poller(self):
        return Poll(self.loop, self.descriptor)

    @cached_property
    def _socket(self):
        socket = self.app.env.socket
        sock = socket.fromfd(self.descriptor, socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(0)
        return sock

    @abstractproperty
    def Connection(self):
        raise NotImplementedError()

    def create_acceptor(self):
        loop = self.loop
        service = self.name
        connections = self._connections
        producer = self.app.worker.create_producer(service)
        socket = self.app.env.socket
        listen_sock = self._socket
        mutex = self.mutex

        @contextmanager
        def ignore_eagain():
            try:
                yield
            except socket.error as exc:
                if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise

        @contextmanager
        def maybe_block():
            if mutex is None:
                yield
            else:
                with mutex:
                    yield

        def on_close(connection):
            connections.remove(connection)

        def on_connection(handle, events, error):
            if error:
                logger.error('Error handling new connection for service %r: %s',
                             service, strerror(error))
                return
            with maybe_block(), ignore_eagain():
                sock, addr = listen_sock.accept()
                client = TCP(loop)
                client.nodelay(True)
                client.open(sock.fileno())
                connection = self.Connection(producer, loop, client, sock, on_close)
                connections.register(connection)

        return on_connection

    def start(self):
        self._poller.start(UV_READABLE, self.create_acceptor())

    def stop(self):
        self._poller.close()
        self._connections.close()
        self._socket.close()


class Acceptors(LoopMixin):
    """Use custom accept loop to prevent main loop
    blocking in accept race between process.

    """

    def __init__(self):
        self._outgoing = deque()
        self._acceptors = set()
        super(Acceptors, self).__init__()

    @cached_property
    def _handle(self):
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

    def register(self, acceptor):
        self._acceptors.add(acceptor)
        self._outgoing.append(acceptor.start)
        self._handle.send()

    @in_loop
    def start(self):
        self._handle.send()

    @in_loop
    def stop(self):
        self._handle.close()
        for acceptor in self._acceptors:
            acceptor.stop()

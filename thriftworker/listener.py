"""Combine device and proxy together. Workers must connect to device backend.

"""
from __future__ import absolute_import

import logging

from pyuv import TCP
from pyuv.error import TCPError
from pyuv.errno import UV_EADDRINUSE, strerror

from .constants import BACKLOG_SIZE
from .exceptions import BindError
from .connection import Connection
from .utils.loop import in_loop
from .utils.decorators import cached_property
from .utils.other import get_addresses_from_pool
from .utils.mixin import LoopMixin

__all__ = ['Listener']

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


class Listener(LoopMixin):
    """Facade for proxy. Support lazy initialization."""

    app = None

    Connections = Connections

    def __init__(self, name, address, backlog=None):
        """Create new listener.

        :param name: service name
        :param address: address of socket
        :param backlog: size of socket connection queue

        """
        self.name = name
        self.address = address
        self.backlog = backlog or BACKLOG_SIZE
        self._connections = self.Connections()

    def _create_socket(self):
        return TCP(self.loop)

    @cached_property
    def _socket(self):
        """A shortcut to create a TCP socket and bind it."""
        return self._create_socket()

    @property
    def host(self):
        """Return host to which this socket is binded."""
        return self._socket.getsockname()[0]

    @property
    def port(self):
        """Return binded port number."""
        return self._socket.getsockname()[1]

    def _create_acceptor(self):
        service = self.name
        loop = self.loop
        connections = self._connections
        producer = self.app.worker.create_producer(service)
        server_socket = self._socket
        socket_factory = self._create_socket

        def on_close(connection):
            connections.remove(connection)

        def on_connection(handle, error):
            if error:
                logger.error('Error handling new connection for service %r: %s',
                             service, strerror(error))
                return
            client = socket_factory()
            client.nodelay(True)
            server_socket.accept(client)
            connection = Connection(producer, loop, client, on_close)
            connections.register(connection)

        return on_connection

    @in_loop
    def start(self):
        binded = False
        socket = self._socket
        acceptor = self._create_acceptor()
        backlog = self.backlog
        addresses = get_addresses_from_pool(self.name, self.address,
                                            self.app.port_range)
        for address in addresses:
            try:
                socket.bind(address)
                socket.listen(acceptor, backlog)
            except TCPError as exc:
                if exc.args[0] == UV_EADDRINUSE:
                    continue
                raise
            else:
                binded = True
                break
        if not binded:
            raise BindError("Service {0!r} can't bind to address {1!r}".
                            format(self.name, self.address))

    @in_loop
    def stop(self):
        self._socket.close()
        self._connections.close()

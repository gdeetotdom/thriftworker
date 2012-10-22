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
from .utils import in_loop, cached_property, get_addresses_from_pool
from .mixin import LoopMixin

__all__ = ['Listener']

logger = logging.getLogger(__name__)


class Listener(LoopMixin):
    """Facade for proxy. Support lazy initialization."""

    app = None

    def __init__(self, name, address, backlog=None):
        """Create new listener.

        :param name: service name
        :param address: address of socket
        :param backlog: size of socket connection queue

        """
        self.name = name
        self.address = address
        self.backlog = backlog or BACKLOG_SIZE

    def create_socket(self):
        return TCP(self.loop)

    @cached_property
    def socket(self):
        """A shortcut to create a TCP socket and bind it."""
        return self.create_socket()

    @property
    def host(self):
        """Return host to which this socket is binded."""
        return self.socket.getsockname()[0]

    @property
    def port(self):
        """Return binded port number."""
        return self.socket.getsockname()[1]

    def _create_acceptor(self):
        service = self.name
        loop = self.loop
        collector = self.app.collector
        producer = self.app.ventilator.create_producer(service)
        server_socket = self.socket
        socket_factory = self.create_socket

        def on_close(connection):
            collector.remove(connection)

        def on_connection(handle, error):
            if error:
                logger.error('Error handling new connection for service %r: %s',
                             service, strerror(error))
                return
            client = socket_factory()
            client.nodelay(True)
            server_socket.accept(client)
            connection = Connection(producer, loop, client, on_close)
            collector.register(connection)

        return on_connection

    @in_loop
    def start(self):
        binded = False
        socket = self.socket
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
        self.socket.close()

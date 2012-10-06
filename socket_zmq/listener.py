"""Combine device and proxy together. Workers must connect to device backend.

"""
from __future__ import absolute_import

from pyuv import TCP
from pyuv.error import TCPError
from pyuv.errno import UV_EADDRINUSE

from .constants import BACKLOG_SIZE
from .exceptions import BindError
from .source import SocketSource
from .utils import in_loop, cached_property, get_addresses_from_pool

__all__ = ['Listener']


class Listener(object):
    """Facade for proxy. Support lazy initialization."""

    app = None

    def __init__(self, name, address, backlog=None):
        """Create new listener.

        :param name: service name
        :param address: address of socket
        :param backlog: size of socket connection queue

        """
        self.name = name
        self.connections = set()
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

    @property
    def loop(self):
        """Shortcut to loop."""
        return self.app.loop

    @property
    def addresses(self):
        return get_addresses_from_pool(self.name, self.address,
                                       self.app.port_range)

    def create_acceptor(self):
        name = self.name
        pool = self.app.sync_pool
        loop = self.loop
        server_socket = self.socket
        socket_factory = self.create_socket
        connections = self.connections

        def on_close(source):
            try:
                connections.remove(source)
            except KeyError:
                pass

        def on_connection(handle, error):
            client = socket_factory()
            client.nodelay(True)
            server_socket.accept(client)
            connections.add(SocketSource(name, pool, loop, client, on_close))

        return on_connection

    @in_loop
    def start(self):
        binded = False
        socket = self.socket
        acceptor = self.create_acceptor()
        backlog = self.backlog
        for address in self.addresses:
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
        while self.connections:
            connection = self.connections.pop()
            if not connection.is_closed():
                connection.close()

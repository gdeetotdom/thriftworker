from __future__ import absolute_import

import logging

from pyuv import TCP
from pyuv.errno import strerror

from .constants import BACKLOG_SIZE
from .connection import Connection
from .utils.loop import in_loop
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


class Acceptor(LoopMixin):

    app = None

    Connections = Connections

    def __init__(self, name, socket, backlog=None):
        self.name = name
        self.socket = socket
        self.backlog = backlog or BACKLOG_SIZE
        self._connections = self.Connections()
        super(Acceptor, self).__init__()

    def _create_acceptor(self):
        service = self.name
        loop = self.loop
        connections = self._connections
        producer = self.app.worker.create_producer(service)
        server_socket = self.socket

        def on_close(connection):
            connections.remove(connection)

        def on_connection(handle, error):
            if error:
                logger.error('Error handling new connection for service %r: %s',
                             service, strerror(error))
                return
            client = TCP(loop)
            client.nodelay(True)
            server_socket.accept(client)
            connection = Connection(producer, loop, client, on_close)
            connections.register(connection)

        return on_connection

    @in_loop
    def start(self):
        acceptor = self._create_acceptor()
        self.socket.listen(acceptor, self.backlog)

    @in_loop
    def stop(self):
        self.socket.close()
        self._connections.close()

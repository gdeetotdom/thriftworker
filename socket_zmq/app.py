from socket_zmq.listener import Listener
from socket_zmq.proxy import Proxy
from socket_zmq.utils import cached_property, SubclassMixin
import pyev
import socket
import zmq
try:
    from billiard import cpu_count
except ImportError:
    cpu_count = lambda: 0

__all__ = ['SocketZMQ']


class SocketZMQ(SubclassMixin):
    """Factory for socket_zmq."""

    def __init__(self, debug=False):
        self.debug = debug
        super(SocketZMQ, self).__init__()

    @cached_property
    def loop(self):
        """Create main event loop."""
        return pyev.Loop(debug=self.debug)

    @cached_property
    def context(self):
        return zmq.Context(cpu_count())

    def Socket(self, address):
        """A shortcut to create a TCP socket and bind it.

        :param address: string consist of <host>:<port>

        """
        sock = socket.socket(family=socket.AF_INET)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
        sock.setblocking(0)
        return sock

    def Proxy(self, name, socket, frontend, pool_size=None, backlog=None):
        """Create new proxy with given params.

        :param name: service name
        :param socket: socket that proxy must listen
        :param frontend: address of frontend zeromq socket
        :param pool_size: size of zeromq pool
        :param backlog: size of socket connection queue

        """
        return Proxy(self.loop, name, socket, self.context, frontend,
                     pool_size, backlog)

    @cached_property
    def Listener(self):
        """Create bounded :class:`Listener` class."""
        return self.subclass_with_self(Listener)

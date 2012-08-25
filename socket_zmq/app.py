"""SocketZMQ.

Distribute thrift requests between workers.

"""
from __future__ import absolute_import

import socket

from pyev import default_loop, recommended_backends
from thrift.protocol import TBinaryProtocol

from .listener import Listener
from .proxy import Proxy
from .worker import Worker
from .utils import cached_property, SubclassMixin, detect_environment

__all__ = ['SocketZMQ']


class SocketZMQ(SubclassMixin):
    """Factory for socket_zmq."""

    def __init__(self, frontend_endpoint=None, backend_endpoint=None,
                 loop=None, context=None, protocol_factory=None):
        # Set provided instance if we can.
        if loop is not None:
            self.loop = loop
        if context is not None:
            self.context = context
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory

        # Use inproc transport by default.
        self.frontend_endpoint = \
            frontend_endpoint or 'inproc://front{0}'.format(id(self))
        self.backend_endpoint = \
            backend_endpoint or 'inproc://back{0}'.format(id(self))

        super(SocketZMQ, self).__init__()

    @cached_property
    def loop(self):
        """Create event loop. Should be running in separate thread."""
        return default_loop(flags=recommended_backends())

    @cached_property
    def context(self):
        """Create ZMQ context. Respect environment."""
        env = detect_environment()
        if env == 'default':
            from zmq.core.context import Context
        elif env == 'gevent':
            from zmq.green import Context
        else:
            raise NotImplementedError('Environment "{0}" not supported'
                                      .format(env))
        return Context.instance()

    @cached_property
    def protocol_factory(self):
        """Specify which protocol should be used."""
        return TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

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

    @cached_property
    def Worker(self):
        """Create bounded :class:`Worker` class."""
        return self.subclass_with_self(Worker)

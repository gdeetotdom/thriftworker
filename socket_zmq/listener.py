"""Combine device and proxy together. Workers must connect to device backend.

"""
from __future__ import absolute_import

import socket

from .proxy import Proxy
from .utils import in_loop, cached_property

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
        self.address = address
        self.backlog = backlog

    @cached_property
    def socket(self):
        """A shortcut to create a TCP socket and bind it."""
        sock = socket.socket(family=socket.AF_INET)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.address)
        sock.setblocking(0)
        return sock

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

    @cached_property
    def proxy(self):
        """Create new proxy with given parameters."""
        return Proxy(self.loop, self.name, self.socket, self.app.sync_pool,
                     self.backlog)

    @in_loop
    def start(self):
        """Start underlying proxy."""
        self.proxy.start()

    @in_loop
    def stop(self):
        """Stop underlying proxy."""
        self.proxy.stop()

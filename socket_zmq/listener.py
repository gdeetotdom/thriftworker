"""Combine device and proxy together. Workers must connect to device backend.

"""
from .utils import in_loop

__all__ = ['Listener']


class Listener(object):
    """Facade for proxy."""

    app = None

    def __init__(self, name, address, frontend=None, pool_size=None, backlog=None):
        self.name = name
        self.loop = self.app.loop
        self.socket = self.app.Socket(address)
        self.frontend = frontend or self.app.frontend_endpoint
        self.proxy = self.app.Proxy(self.name, self.socket, self.frontend,
                                    pool_size, backlog)

    @property
    def host(self):
        """Return host to which this socket is binded."""
        return self.socket.getsockname()[0]

    @property
    def port(self):
        """Return binded port number."""
        return self.socket.getsockname()[1]

    @in_loop
    def start(self):
        """Start underlying proxy."""
        self.proxy.start()

    @in_loop
    def stop(self):
        """Stop underlying proxy."""
        self.proxy.stop()

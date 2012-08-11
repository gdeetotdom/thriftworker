"""Combine device and proxy together. Workers must connect to device backend.

"""
from socket_zmq.utils import in_loop

__all__ = ['Listener']


class Listener(object):
    """Combine proxy and device together."""

    app = None

    def __init__(self, name, address, frontend, pool_size=None, backlog=None):
        self.name = name
        self.loop = self.app.loop
        self.socket = self.app.Socket(address)
        self.frontend = frontend
        self.proxy = self.app.Proxy(self.name, self.socket, self.frontend,
                                    pool_size, backlog)

    @property
    def host(self):
        return self.socket.getsockname()[0]

    @property
    def port(self):
        return self.socket.getsockname()[1]

    @in_loop
    def start(self):
        self.proxy.start()

    @in_loop
    def stop(self):
        self.proxy.stop()

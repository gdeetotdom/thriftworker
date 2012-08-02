"""Combine device and proxy together. Workers must connect to device backend.

"""
from socket_zmq.utils import in_loop
import uuid

__all__ = ['Listener']


class Listener(object):
    """Combine proxy and device together."""

    app = None

    def __init__(self, address, backend, pool_size=None, backlog=None):
        self.loop = self.app.loop
        self.frontend = 'inproc://{0}'.format(uuid.uuid4().hex)
        self.device = self.app.Device(self.frontend, backend)
        self.proxy = self.app.Proxy(address, self.frontend, pool_size, backlog)

    @in_loop
    def start(self):
        self.device.start()
        self.proxy.start()

    @in_loop
    def stop(self):
        self.proxy.stop()

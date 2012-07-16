from socket_zmq.proxy import Proxy
from socket_zmq.utils import cached_property, SubclassMixin
from socket_zmq.worker import Worker
from zmq.devices import ThreadDevice
import _socket
import pyev
import socket
import zmq

__all__ = ['Application']


class Component(object):
    """Base component."""

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class ProxyComponent(object):
    """Describe proxy component."""

    app = None

    def __init__(self, address, frontend, backend, pool_size=None, backlog=None):
        self.device = self.app.Device(frontend, backend)
        self.proxy = self.app.Proxy(address, frontend, pool_size, backlog)

    def start(self):
        self.device.start()
        self.proxy.start()

    def stop(self):
        self.proxy.stop()


class Controller(object):
    """Holder for components."""

    RUN = 0x1
    CLOSE = 0x2

    app = None

    def __init__(self):
        self._state = None
        self.loop = self.app.loop
        self.components = set()

    def register(self, component):
        self.components.add(component)

        if self._state == self.RUN:
            # if we are running start component here
            component.start()

    def unregister(self, component):
        self.components.remove(component)

        if self._state == self.RUN:
            # if we are running stop component here
            component.stop()

    def start(self):
        self._state = self.RUN

        # start all components
        for component in self.components:
            component.start()

        # start main loop
        self.loop.start()

    def stop(self):
        # we are already stopping
        if self._state in (self.CLOSE,):
            return

        self._state = self.CLOSE

        # stop main loop
        self.loop.stop(pyev.EVBREAK_ALL)

        # stop all components
        for component in reversed(self.components):
            component.stop()

    def serve_forever(self):
        try:
            self.start()
        finally:
            self.stop()


class Application(SubclassMixin):
    """Factory for socket_zmq."""

    def __init__(self, debug=False):
        self.debug = debug

    @cached_property
    def loop(self):
        return pyev.Loop(debug=self.debug)

    @cached_property
    def context(self):
        return zmq.Context()

    def Socket(self, address):
        """A shortcut to create a TCP socket and bind it.

        :param address: string consist of <host>:<port>

        """
        sock = socket.socket(family=_socket.AF_INET)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
        sock.setblocking(0)
        return sock

    def Proxy(self, address, frontend, pool_size=None, backlog=None):
        """Create new proxy with given params.

        :param address: string consist of <host>:<port>
        :param frontend: address of frontend zeromq socket
        :param pool_size: size of zeromq pool
        :param backlog: size of socket connection queue

        """
        return Proxy(self.loop, self.Socket(address),
                     self.context, frontend, pool_size, backlog)

    def Device(self, frontend, backend):
        """Create zmq device.

        :param frontend: address of frontend socket
        :param backend: address of backend socket

        """
        device = ThreadDevice(zmq.QUEUE, zmq.ROUTER, zmq.DEALER)
        device.context_factory = lambda: self.context
        device.bind_in(frontend)
        device.bind_out(backend)
        return device

    def Worker(self, processor, backend):
        """Create new worker.

        :param processor: message processor
        :param backend: address of backend socket

        """
        return Worker(self.context, backend, processor)

    @cached_property
    def ProxyComponent(self):
        """Create :class:`ProxyComponent` subclass."""
        return self.subclass_with_self(ProxyComponent)

    @cached_property
    def Controller(self):
        """Create :class:`Controller` subclass."""
        return self.subclass_with_self(Controller)

    @cached_property
    def controller(self):
        """Create instance of :class:`Controller`."""
        return self.Controller()

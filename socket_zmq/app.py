"""SocketZMQ.

Distribute thrift requests between workers.

"""
from __future__ import absolute_import

from pyuv import Loop
from thrift.protocol import TBinaryProtocol

from .collector import Collector
from .listener import Listener
from .loop import LoopContainer
from .worker import Worker
from .ventilator import Ventilator
from .utils import cached_property, SubclassMixin

__all__ = ['SocketZMQ']


class SocketZMQ(SubclassMixin):
    """Factory for socket_zmq."""

    def __init__(self, loop=None, context=None, protocol_factory=None,
                 pool_size=None, port_range=None):
        # Set provided instance if we can.
        if loop is not None:
            self.loop = loop
        if context is not None:
            self.context = context
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory
        self.port_range = port_range
        super(SocketZMQ, self).__init__()

    @cached_property
    def loop(self):
        """Create event loop. Should be running in separate thread."""
        return Loop()

    @cached_property
    def LoopContainer(self):
        """Create bounded :class:`LoopContainer` class."""
        return self.subclass_with_self(LoopContainer)

    @cached_property
    def loop_container(self):
        """Instance of bounded :class:`LoopContainer`."""
        return self.LoopContainer()

    @cached_property
    def protocol_factory(self):
        """Specify which protocol should be used."""
        return TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

    @cached_property
    def Ventilator(self):
        """Create bounded :class:`Ventilator` class."""
        return self.subclass_with_self(Ventilator)

    @cached_property
    def ventilator(self):
        """Create new ventilator."""
        return self.Ventilator()

    @cached_property
    def Collector(self):
        """Create bounded :class:`Collector` class."""
        return self.subclass_with_self(Collector)

    @cached_property
    def collector(self):
        """Create bounded :class:`Collector` class."""
        return self.Collector()

    @cached_property
    def Listener(self):
        """Create bounded :class:`Listener` class."""
        return self.subclass_with_self(Listener)

    @cached_property
    def Worker(self):
        """Create bounded :class:`Worker` class."""
        return self.subclass_with_self(Worker)

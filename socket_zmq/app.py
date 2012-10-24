"""SocketZMQ.

Distribute thrift requests between workers.

"""
from __future__ import absolute_import

from pyuv import Loop
from thrift.protocol import TBinaryProtocol

from .state import set_current_app, get_current_app
from .listener import Listener
from .loop import LoopContainer
from .processor import Processor
from .utils.decorators import cached_property
from .utils.mixin import SubclassMixin

__all__ = ['SocketZMQ']


class SocketZMQ(SubclassMixin):
    """Factory for socket_zmq."""

    pool_cls = 'socket_zmq.strategies.solo:SoloPool'

    def __init__(self, protocol_factory=None, port_range=None):
        # Set provided instance if we can.
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory
        self.port_range = port_range
        super(SocketZMQ, self).__init__()
        set_current_app(self)

    @staticmethod
    def instance():
        """Return global instance."""
        return get_current_app()

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
    def Processor(self):
        """Create bounded :class:`Processor` class."""
        return self.subclass_with_self(Processor)

    @cached_property
    def processor(self):
        """Create global request processor instance."""
        return self.Processor()

    @cached_property
    def Pool(self):
        """Create bounded pool class."""
        return self.subclass_with_self(self.pool_cls, reverse='Pool')

    @cached_property
    def pool(self):
        """Create pool."""
        return self.Pool()

    @cached_property
    def Listener(self):
        """Create bounded :class:`Listener` class."""
        return self.subclass_with_self(Listener)

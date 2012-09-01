"""SocketZMQ.

Distribute thrift requests between workers.

"""
from __future__ import absolute_import

from pyev import default_loop, recommended_backends
from thrift.protocol import TBinaryProtocol

from .constants import POOL_SIZE, DEFAULT_ENV, GEVENT_ENV
from .device import Device
from .listener import Listener
from .loop import LoopContainer
from .pool import SinkPool
from .worker import Worker
from .utils import cached_property, SubclassMixin, detect_environment

__all__ = ['SocketZMQ']


class SocketZMQ(SubclassMixin):
    """Factory for socket_zmq."""

    def __init__(self, loop=None, context=None, protocol_factory=None,
                 pool_size=None):
        # Set provided instance if we can.
        if loop is not None:
            self.loop = loop
        if context is not None:
            self.context = context
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory
        # Worker endpoint list.
        self.worker_endpoints = []
        self.pool_size = pool_size or POOL_SIZE
        super(SocketZMQ, self).__init__()

    @cached_property
    def loop(self):
        """Create event loop. Should be running in separate thread."""
        return default_loop(flags=recommended_backends())

    @cached_property
    def loop_container(self):
        """Instance of bounded :class:`LoopContainer`."""
        return self.LoopContainer()

    @cached_property
    def context(self):
        """Create ZMQ context. Respect environment."""
        env = detect_environment()
        if env == DEFAULT_ENV:
            from zmq.core.context import Context
        elif env == GEVENT_ENV:
            from zmq.green import Context
        else:
            raise NotImplementedError('Environment "{0}" not supported'
                                      .format(env))
        return Context.instance()

    @cached_property
    def protocol_factory(self):
        """Specify which protocol should be used."""
        return TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

    @cached_property
    def sync_pool(self):
        """Instance of :class:`SyncPool`."""
        return SinkPool(self.loop, self.context, self.worker_endpoints,
                        self.pool_size)

    @cached_property
    def device(self):
        """Instance of bounded :class:`Device`."""
        return self.Device()

    @cached_property
    def LoopContainer(self):
        """Create bounded :class:`LoopContainer` class."""
        return self.subclass_with_self(LoopContainer)

    @cached_property
    def Listener(self):
        """Create bounded :class:`Listener` class."""
        return self.subclass_with_self(Listener)

    @cached_property
    def Device(self):
        """Create bounded :class:`Device` class."""
        return self.subclass_with_self(Device)

    @cached_property
    def Worker(self):
        """Create bounded :class:`Worker` class."""
        return self.subclass_with_self(Worker)

"""ThriftWorker.

Distribute thrift requests between workers.

"""
from __future__ import absolute_import

from pyuv import Loop
from thrift.protocol import TBinaryProtocol

from . import constants
from .state import set_current_app, get_current_app
from .listener import Listener
from .loop import LoopContainer
from .services import Services
from .utils.decorators import cached_property
from .utils.mixin import SubclassMixin
from .utils.env import detect_environment


class ThriftWorker(SubclassMixin):
    """Store global application state. Also acts as factory for whole
    application.

    """

    acceptor_cls = 'thriftworker.transports.framed:FramedAcceptor'

    def __init__(self, loop=None, protocol_factory=None, port_range=None):
        # Set provided instance if we can.
        if loop is not None:
            self.loop = loop
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory
        self.port_range = port_range
        super(ThriftWorker, self).__init__()
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
    def Env(self):
        cls = constants.ENVS[detect_environment()]
        return self.subclass_with_self(cls, reverse='Env')

    @cached_property
    def env(self):
        return self.Env()

    @cached_property
    def Services(self):
        """Create bounded :class:`Processor` class."""
        return self.subclass_with_self(Services)

    @cached_property
    def services(self):
        """Create global request processor instance."""
        return self.Services()

    @cached_property
    def Listener(self):
        """Create bounded :class:`Listener` class."""
        return self.subclass_with_self(Listener)

    @cached_property
    def Acceptor(self):
        return self.subclass_with_self(self.acceptor_cls, reverse='Acceptor')

    @cached_property
    def Worker(self):
        cls = constants.WORKERS[detect_environment()]
        return self.subclass_with_self(cls, reverse='Worker')

    @cached_property
    def worker(self):
        """Create pool."""
        return self.Worker()

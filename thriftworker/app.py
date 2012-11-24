"""ThriftWorker.

Distribute thrift requests between workers.

"""
from __future__ import absolute_import

import logging

from pyuv import Loop
from thrift.protocol import TBinaryProtocol

from . import constants
from .transports.base import Acceptors
from .state import set_current_app, get_current_app
from .listener import Listener, Listeners
from .loop import LoopContainer
from .services import Services
from .utils.decorators import cached_property
from .utils.mixin import SubclassMixin
from .utils.env import detect_environment

logger = logging.getLogger(__name__)


class ThriftWorker(SubclassMixin):
    """Store global application state. Also acts as factory for whole
    application.

    """

    acceptor_cls = 'thriftworker.transports.framed:FramedAcceptor'

    def __init__(self, loop=None, protocol_factory=None, port_range=None,
                 pool_size=None):
        # Set provided instance if we can.
        if loop is not None:
            self.loop = loop
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory
        self.port_range = port_range
        if pool_size is not None and pool_size < 0:
            raise ValueError('Pool size can not be negative.')
        self.pool_size = pool_size or 1
        super(ThriftWorker, self).__init__()
        set_current_app(self)

    @staticmethod
    def instance():
        """Return global instance."""
        return get_current_app()

    @cached_property
    def loop(self):
        """Create event loop. Should be running in separate thread."""
        return Loop.default_loop()

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

    @property
    def env_cls(self):
        env = detect_environment()
        if env == constants.GEVENT_ENV:
            return 'thriftworker.envs.green:GeventEnv'
        elif env == constants.DEFAULT_ENV:
            return 'thriftworker.envs.sync:SyncEnv'
        else:
            raise NotImplementedError()

    @cached_property
    def Env(self):
        return self.subclass_with_self(self.env_cls, reverse='Env')

    @cached_property
    def env(self):
        logger.debug('Using {0!r} env'.format(self.Env))
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
    def Listeners(self):
        """Create bounded :class:`Listeners` class."""
        return self.subclass_with_self(Listeners)

    @cached_property
    def listeners(self):
        """Create pool of listeners."""
        return self.Listeners()

    @cached_property
    def Acceptor(self):
        return self.subclass_with_self(self.acceptor_cls, reverse='Acceptor')

    @cached_property
    def Acceptors(self):
        return self.subclass_with_self(Acceptors)

    @cached_property
    def acceptors(self):
        """Create pool of acceptors."""
        return self.Acceptors()

    @property
    def worker_cls(self):
        env = detect_environment()
        if env == constants.GEVENT_ENV:
            return 'thriftworker.workers.green:GeventWorker'
        elif env == constants.DEFAULT_ENV:
            if self.pool_size == 1:
                return 'thriftworker.workers.sync:SyncWorker'
            else:
                return 'thriftworker.workers.threads:ThreadsWorker'
        else:
            raise NotImplementedError()

    @cached_property
    def Worker(self):
        return self.subclass_with_self(self.worker_cls, reverse='Worker')

    @cached_property
    def worker(self):
        """Create some worker routine."""
        logger.debug('Using {0!r} worker'.format(self.Worker))
        return self.Worker()

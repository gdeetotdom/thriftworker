"""ThriftWorker.

Distribute thrift requests between workers.

"""
from __future__ import absolute_import

import logging

from pyuv import Loop
from thrift.protocol import TBinaryProtocol

from .transports.base import Acceptors
from .state import set_current_app, get_current_app
from .listener import Listener, Listeners
from .hub import Hub
from .services import Services
from .utils.decorators import cached_property
from .utils.mixin import SubclassMixin
from .utils.stats import Counters, Timers

logger = logging.getLogger(__name__)


class ThriftWorker(SubclassMixin):
    """Store global application state. Also acts as factory for whole
    application.

    """

    acceptor_cls = 'thriftworker.transports.framed:FramedAcceptor'

    def __init__(self, loop=None, protocol_factory=None, port_range=None,
                 pool_size=None, shutdown_timeout=None):
        self.counters = Counters()
        self.timeouts = Timers()
        self.execution_timers = Timers()
        self.dispatching_timers = Timers()
        # Set provided instance if we can.
        if loop is not None:
            self.loop = loop
        if protocol_factory is not None:
            self.protocol_factory = protocol_factory
        self.port_range = port_range
        self.pool_size = pool_size
        self.shutdown_timeout = shutdown_timeout or 30.0
        super(ThriftWorker, self).__init__()
        set_current_app(self)

    @cached_property
    def pool_size(self):
        """Return default pool size."""
        return 1

    @pool_size.setter
    def pool_size(self, value):
        if value is not None and value < 0:
            raise ValueError('Pool size can not be negative.')
        return int(value or 1) or 1

    @cached_property
    def port_range(self):
        """Return range from which we allowed to allocate ports."""
        return None

    @port_range.setter
    def port_range(self, value):
        if value is None:
            return None
        try:
            start, stop = int(value[0]), int(value[1])
        except (IndexError, ValueError):
            raise ValueError('Port range must be tuple of two integers.')
        return (start, stop)

    @cached_property
    def protocol_factory(self):
        """Specify which protocol should be used."""
        return TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

    @classmethod
    def default(cls):
        """Return default application instance."""
        try:
            app = cls.instance()
        except RuntimeError:
            app = cls()
        return app

    @staticmethod
    def instance():
        """Return global instance."""
        return get_current_app()

    @cached_property
    def loop(self):
        """Create event loop. Should be running in separate thread."""
        return Loop()

    @cached_property
    def Hub(self):
        """Create bounded :class:`Hub` class."""
        return self.subclass_with_self(Hub)

    @cached_property
    def hub(self):
        """Instance of bounded :class:`LoopContainer`."""
        return self.Hub()

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
        if self.pool_size == 1:
            return 'thriftworker.workers.sync:SyncWorker'
        else:
            return 'thriftworker.workers.threads:ThreadsWorker'

    @cached_property
    def Worker(self):
        return self.subclass_with_self(self.worker_cls, reverse='Worker')

    @cached_property
    def worker(self):
        """Create some worker routine."""
        logger.debug('Using {0!r} worker'.format(self.Worker))
        return self.Worker(self.pool_size)

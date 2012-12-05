"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

import logging

from pyuv import Async
from pyuv.error import HandleError

from .utils.loop import in_loop
from .utils.mixin import LoopMixin
from .utils.decorators import cached_property

__all__ = ['LoopContainer']

logger = logging.getLogger(__name__)


class LoopContainer(LoopMixin):
    """Container for event loop."""

    app = None

    def __init__(self):
        self._guard = None

    @cached_property
    def _started(self):
        return self.app.env.RealEvent()

    @cached_property
    def _stopped(self):
        return self.app.env.RealEvent()

    def wakeup(self):
        assert self._guard is not None, 'loop not started'
        try:
            self._guard.send()
        except HandleError:
            pass  # pragma: no cover

    def _error_handle(self, exc_type, value, traceback):
        logger.exception(value, exc_info=(exc_type, value, traceback))

    def _configure_loop(self, loop):
        setattr(loop, 'ident', self.app.env.get_real_ident())
        loop.excepthook = self._error_handle

    def _run(self):
        """Run event loop."""
        loop = self.loop
        self._configure_loop(loop)
        logger.debug('Loop #%r started...', id(loop))
        self._started.set()
        try:
            loop.run()
            logger.debug('Loop #%r stopped...', id(loop))
            self._stopped.set()
        except Exception as exc:
            logger.exception(exc)

    @in_loop
    def _close_handlers(self):
        """Close all stale handlers."""
        def cb_handle(handle):
            if not handle.closed:
                logger.debug('Close stale handle %r', handle)
                handle.close()
        self.loop.walk(cb_handle)

    def start(self):
        """Start event loop in separate thread."""
        assert self._guard is None
        # Cleanup events.
        self._started.clear()
        self._stopped.clear()
        # Prevent loop exit.
        async = self._guard = Async(self.loop, lambda h: None)
        async.send()
        # Start loop in separate thread.
        self.app.env.start_real_thread(self._run)
        self._started.wait()

    def stop(self):
        """Stop event loop and wait until it exit."""
        assert self._guard is not None
        self.wakeup()
        self._close_handlers()
        if self._guard.active:
            self._guard.close()
        self._guard = None
        self._stopped.wait()

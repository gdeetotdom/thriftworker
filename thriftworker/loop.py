"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

import logging

from threading import Event
from pyuv import Async

from .utils.threads import spawn, get_ident
from .utils.loop import in_loop
from .utils.mixin import LoopMixin

__all__ = ['LoopContainer']

logger = logging.getLogger(__name__)


class LoopContainer(LoopMixin):
    """Container for event loop."""

    app = None

    def __init__(self):
        self._guard = None
        self._started = Event()
        self._stopped = Event()

    def _run(self):
        """Run event loop."""
        loop = self.loop
        setattr(loop, 'ident', get_ident())
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
        async = self._guard = Async(self.loop, lambda h: None)
        async.send()
        spawn(self._run)
        self._started.wait()

    def stop(self):
        """Stop event loop and wait until it exit."""
        assert self._guard is not None, 'loop not started'
        if self._guard.active:
            self._guard.close()
        self._close_handlers()
        self._stopped.wait()

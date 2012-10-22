"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

import logging

from pyuv import Async

from .utils import spawn, Event, in_loop
from .mixin import LoopMixin

__all__ = ['LoopContainer']

logger = logging.getLogger(__name__)


class LoopContainer(LoopMixin):
    """Container for event loop."""

    app = None

    def __init__(self):
        self._guard_watcher = None
        self._started = Event()
        self._stopped = Event()

    def _run(self):
        """Run event loop."""
        self._started.set()
        loop = self.loop
        logger.debug('Loop %r started', loop)
        try:
            loop.run()
            logger.debug('Loop %r stopped', loop)
            self._stopped.set()
        except Exception as exc:
            logger.exception(exc)

    def start(self):
        """Start event loop in separate thread."""
        self._guard_watcher = Async(self.loop, lambda *args: None)
        spawn(self._run)
        self._started.wait()

    def cb_handle(self, handle):
        if not handle.closed:
            logger.warning('Close stale handle %r', handle)
            handle.close()

    @in_loop
    def close_handlers(self):
        """Close all stale handlers."""
        self.loop.walk(self.cb_handle)

    def stop(self):
        """Stop event loop and wait until it exit."""
        assert self._guard_watcher is not None, 'loop not started'
        self._guard_watcher.close()
        self.close_handlers()
        self._stopped.wait()

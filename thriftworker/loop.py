"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

import logging
from collections import deque
from functools import partial

from pyuv import Async, Prepare
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
        self._outgoing = deque()

    @cached_property
    def _started(self):
        return self.app.env.RealEvent()

    @cached_property
    def _stopped(self):
        return self.app.env.RealEvent()

    @cached_property
    def _guard(self):
        """Prevent loop exit there is nothing to do."""
        return Async(self.loop, lambda h: None)

    def wakeup(self):
        assert self._guard is not None, 'loop not started'
        try:
            self._guard.send()
        except HandleError:
            pass  # pragma: no cover

    @cached_property
    def _callback_handle(self):
        """Handle that should run functions in loop thread."""
        return Prepare(self.loop)

    def _before_iteration(self, handle):
        """Should be used to run functions in loop's thread."""
        outgoing = self._outgoing
        while True:
            try:
                callback = outgoing.popleft()
            except IndexError:
                break
            else:
                callback()

    def callback(self, fn, *args, **kwargs):
        """Enqueue function execution to loop."""
        self._outgoing.append(partial(fn, *args, **kwargs))
        self.wakeup()

    def _error_handle(self, exc_type, value, traceback):
        """Log in-loop errors with our logger."""
        logger.exception(value, exc_info=(exc_type, value, traceback))

    def _configure_loop(self, loop):
        loop.ident = self.app.env.get_real_ident()
        loop.excepthook = self._error_handle
        self._callback_handle.start(self._before_iteration)

    def _run(self):
        """Run event loop."""
        loop = self.loop
        started = self._started
        stopped = self._stopped
        self._configure_loop(loop)
        logger.debug('Loop #%r started...', id(loop))
        started.set()
        try:
            loop.run()
            logger.debug('Loop #%r stopped...', id(loop))
        except Exception as exc:
            logger.exception(exc)
        finally:
            stopped.set()

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
        # Cleanup events.
        self._started.clear()
        self._stopped.clear()
        # Prevent loop exit.
        self._guard.send()
        # Start loop in separate thread.
        self.app.env.start_real_thread(self._run)
        self._started.wait()

    def stop(self):
        """Stop event loop and wait until it exit."""
        self.wakeup()
        self._close_handlers()
        del self._guard
        del self._callback_handle
        self._stopped.wait()

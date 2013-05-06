from __future__ import absolute_import

import logging
from functools import partial
from threading import Event
from thread import start_new_thread, get_ident

from six import PY3
from greenlet import greenlet, getcurrent
from pyuv import Async
from pyuv.error import HandleError

from ..utils.loop import in_loop
from ..utils.mixin import LoopMixin
from ..utils.decorators import cached_property

from .waiter import Waiter
from .task import Greenlet
from .queue import AsyncQueue

logger = logging.getLogger(__name__)


class LoopGreenlet(greenlet):
    """Greenlet that execute main loop."""

    def __init__(self, loop):
        self.loop = loop
        greenlet.__init__(self)

    def run(self):
        try:
            self.loop.run()
        except Exception as exc:
            logger.exception(exc)

    def switch(self):
        switch_out = getattr(getcurrent(), 'switch_out', None)
        if switch_out is not None:
            switch_out()
        return greenlet.switch(self)

    def switch_out(self):
        raise AssertionError('Impossible to call blocking function in the'
                             ' event loop callback')


class Callback(object):
    """Callback event return by :meth:`Hub.callback` that store function
    arguments and function itself.

    """

    __slots__ = ['run', 'args', 'kwargs', 'alive']

    def __init__(self, run, *args, **kwargs):
        self.run = run
        self.args = args
        self.kwargs = kwargs
        self.alive = True

    def __call__(self):
        if not self.alive:
            return
        try:
            return self.run(*self.args, **self.kwargs)
        except Exception as exc:
            logging.exception(exc)
        finally:
            self.stop()

    if PY3:
        def __bool__(self):
            return self.alive
    else:
        def __nonzero__(self):
            return self.alive

    def stop(self):
        """Prevent callback execution if it's not already called."""
        self.alive = False
        self.run = self.args = self.kwargs = None


class Hub(LoopMixin):
    """Container for event loop."""

    app = None

    def __init__(self):
        self.Waiter = partial(Waiter, self)
        self.Greenlet = partial(Greenlet, self)
        self._started = Event()
        self._stopped = Event()

    @cached_property
    def _async_queue(self):
        """Create async queue here."""
        return AsyncQueue(self.loop)

    @cached_property
    def _greenlet(self):
        """Greenlet in which we run loop."""
        return LoopGreenlet(self.loop)

    @cached_property
    def _guard(self):
        """Prevent loop exit there is nothing to do."""
        return Async(self.loop, lambda h: None)

    def wakeup(self):
        """Send wakeup signal to loop."""
        assert self._guard is not None, 'loop not started'
        try:
            self._guard.send()
        except HandleError:
            pass  # pragma: no cover

    def callback(self, fn, *args, **kwargs):
        """Enqueue function execution to loop. Return :class:`Callback`
        instance.

        """
        cb = Callback(fn, *args, **kwargs)
        self._async_queue.send(cb)
        return cb

    def handle_error(self, exc_type, value, traceback):
        """Log in-loop errors with our logger."""
        logging.error(value, exc_info=(exc_type, value, traceback))

    def _setup_loop(self, loop):
        loop.ident = get_ident()
        loop.excepthook = self.handle_error
        return loop

    def _teardown_loop(self, loop):
        loop.excepthook = None
        self._async_queue.close()
        del self._greenlet
        del self._guard

    def _run(self):
        """Run event loop."""
        loop = self.loop
        started = self._started
        stopped = self._stopped
        greenlet = self._greenlet
        loop = self._setup_loop(loop)
        logger.debug('Loop %s started...', hex(id(loop)))
        started.set()
        try:
            greenlet.switch()
        finally:
            logger.debug('Loop %s stopped...', hex(id(loop)))
            stopped.set()
            self._teardown_loop(loop)

    @in_loop
    def _close_handlers(self):
        """Close all stale handlers."""
        def cb_handle(handle):
            if not getattr(handle, 'bypass', False) and not handle.closed:
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
        start_new_thread(self._run, ())
        self._started.wait()

    def stop(self):
        """Stop event loop and wait until it exit."""
        self.wakeup()
        self._close_handlers()
        self._stopped.wait()

    def switch(self):
        """Switch into the loop's greenlet."""
        return self._greenlet.switch()

    def spawn(self, *args, **kwargs):
        """Return a new :class:`Greenlet` object, scheduled to start.

        The arguments are passed to :meth:`Greenlet.__init__`.

        """
        assert self.app.loop.ident == get_ident(), \
            "greenlet spawned from non-loop thread"
        g = self.Greenlet(*args, **kwargs)
        g.start()
        return g

    def wait(self, watcher, *args, **kwargs):
        """Wait for given watcher."""
        waiter = self.Waiter()
        unique = object()

        def inner_callback(*args):
            waiter.switch(unique)

        watcher.start(inner_callback, *args, **kwargs)
        try:
            result = waiter.get()
            assert result is unique, \
                'Invalid switch into %s: %r (expected %r)' % \
                    (getcurrent(), result, unique)
        finally:
            watcher.stop()

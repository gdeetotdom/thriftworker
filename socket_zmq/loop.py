"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

import logging

from .utils import spawn, in_loop, Event

__all__ = ['LoopContainer']


class LoopContainer(object):
    """Container for event loop."""

    app = None

    def __init__(self):
        self._guard_watcher = None
        self._started = Event()

    @property
    def loop(self):
        """Shortcut to loop."""
        return self.app.loop

    def _run(self):
        """Run event loop."""
        self._started.set()
        try:
            self.app.loop.start()
        except Exception as exc:
            logging.exception(exc)

    def start(self):
        """Start event loop in separate thread."""
        watcher = self._guard_watcher = self.loop.async(lambda *args: None)
        watcher.start()
        spawn(self._run)
        self._started.wait()

    @in_loop
    def _shutdown(self):
        """Shutdown event loop."""
        self._guard_watcher.stop()
        self.loop.stop()

    def stop(self):
        """Stop event loop and wait until it exit."""
        assert self._guard_watcher is not None, 'loop not started'
        self._shutdown()

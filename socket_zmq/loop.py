"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

import logging

from pyuv import Async

from .utils import spawn, Event

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
            self.app.loop.run()
        except Exception as exc:
            logging.exception(exc)

    def start(self):
        """Start event loop in separate thread."""
        self._guard_watcher = Async(self.loop, lambda *args: None)
        spawn(self._run)
        self._started.wait()

    def stop(self):
        """Stop event loop and wait until it exit."""
        assert self._guard_watcher is not None, 'loop not started'
        self._guard_watcher.close()

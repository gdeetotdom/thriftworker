"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

from .utils import spawn, in_loop

__all__ = ['LoopContainer']


class LoopContainer(object):
    """Container for event loop."""

    app = None

    def __init__(self):
        self._guard_watcher = None

    @property
    def loop(self):
        """Shortcut to loop."""
        return self.app.loop

    def _run(self):
        """Run event loop."""
        self.app.loop.start()

    def start(self):
        """Start event loop in separate thread."""
        watcher = self._guard_watcher = self.loop.async(lambda *args: None)
        watcher.start()
        spawn(self._run)

    @in_loop
    def _shutdown(self):
        """Shutdown event loop."""
        self._guard_watcher.stop()
        self.loop.stop()

    def stop(self):
        """Stop event loop and wait until it exit."""
        assert self._guard_watcher is not None, 'loop not started'
        self._shutdown()

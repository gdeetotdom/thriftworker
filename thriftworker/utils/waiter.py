"""Some useful tools for components."""
from __future__ import absolute_import

import logging

from threading import Event

logger = logging.getLogger(__name__)


class Aborted(Exception):
    """Waiting was aborted."""


class Waiter(object):
    """Waiter primitive."""

    def __init__(self, timeout=None):
        self.timeout = timeout or 30
        self._aborted = False
        self._event = Event()
        super(Waiter, self).__init__()

    def reset(self):
        """Reset waiter state."""
        self._aborted = False
        self._event.clear()

    def abort(self):
        """Abort initialization."""
        self._aborted = True
        self._event.set()

    def done(self):
        """Notify all that initialization done."""
        self._event.set()

    def wait(self):
        """Wait for initialization."""
        event = self._event
        try:
            event.wait(self.timeout)
            if self._aborted:
                raise Aborted('Waiter was aborted!')
            return event.is_set()
        finally:
            self.reset()

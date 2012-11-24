from __future__ import absolute_import

from thread import start_new_thread
from threading import Event, RLock

from .base import BaseEnv


class SyncEnv(BaseEnv):
    """Default CPython environment."""

    @property
    def Event(self):
        return Event

    @property
    def RLock(self):
        return RLock

    def start_thread(self, func, args=None, kwargs=None):
        return start_new_thread(func, args or (), kwargs or {})

from __future__ import absolute_import

import socket

from thread import start_new_thread, get_ident
from threading import Event, RLock

from .base import BaseEnv


class SyncEnv(BaseEnv):
    """Default CPython environment."""

    @property
    def socket(self):
        return socket

    @property
    def Event(self):
        return Event
    RealEvent = Event

    @property
    def RLock(self):
        return RLock

    def start_thread(self, func, args=None, kwargs=None):
        return start_new_thread(func, args or (), kwargs or {})
    start_real_thread = start_thread

    def get_real_ident(self):
        return get_ident()

    def get_ident(self):
        return get_ident()

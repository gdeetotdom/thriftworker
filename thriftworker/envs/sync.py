from __future__ import absolute_import

from threading import Event, RLock

from .base import BaseEnv


class SyncEnv(BaseEnv):

    @property
    def Event(self):
        return Event

    @property
    def RLock(self):
        return RLock

from __future__ import absolute_import

from threading import Event

from .base import BaseEnv


class SyncEnv(BaseEnv):

    @property
    def Event(self):
        return Event

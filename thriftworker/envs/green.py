from __future__ import absolute_import

from gevent.event import Event
from gevent.coros import RLock

from .base import BaseEnv


class GeventEnv(BaseEnv):

    @property
    def Event(self):
        return Event

    @property
    def RLock(self):
        return RLock
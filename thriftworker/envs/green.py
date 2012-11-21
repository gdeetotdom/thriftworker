from __future__ import absolute_import

from gevent.event import Event
from gevent.coros import RLock
from gevent.thread import start_new_thread

from .base import BaseEnv


class GeventEnv(BaseEnv):

    @property
    def Event(self):
        return Event

    @property
    def RLock(self):
        return RLock

    def start_thread(self, func, args=None, kwargs=None):
        return start_new_thread(func, args or (), kwargs or {})

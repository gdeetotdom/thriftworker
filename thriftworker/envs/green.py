from __future__ import absolute_import

from gevent.event import Event

from .base import BaseEnv


class GeventEnv(BaseEnv):

    @property
    def Event(self):
        return Event

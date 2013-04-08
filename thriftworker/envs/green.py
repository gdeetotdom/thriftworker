from __future__ import absolute_import

from gevent.event import Event
from gevent.coros import RLock
from gevent.thread import start_new_thread, get_ident

from thriftworker.utils.imports import get_real_module
from thriftworker.utils.decorators import cached_property

from .base import BaseEnv


class GeventEnv(BaseEnv):
    """Implementation of gevent-compatible environment."""

    @cached_property
    def socket(self):
        return get_real_module('socket')

    @cached_property
    def thread(self):
        return get_real_module('thread')

    @property
    def _start_real_thread(self):
        return self.thread.start_new_thread

    def start_real_thread(self, func, args=None, kwargs=None):
        return self._start_real_thread(func, args or (), kwargs or {})

    @property
    def get_real_ident(self):
        return self.thread.get_ident

    @property
    def Event(self):
        return Event

    @property
    def RLock(self):
        return RLock

    def start_thread(self, func, args=None, kwargs=None):
        return start_new_thread(func, args or (), kwargs or {})

    def get_ident(self):
        return get_ident()

from __future__ import absolute_import

from gevent.event import Event
from gevent.coros import RLock
from gevent.thread import start_new_thread
from gevent.monkey import get_original

from thriftworker.utils.imports import get_real_module
from thriftworker.utils.decorators import cached_property

from .base import BaseEnv


def _recreate_threading():
    """Create new threading module without monkey patch."""
    threading = get_real_module('threading')
    threading._time = get_original('time', 'time')
    threading._sleep = get_original('time', 'sleep')
    threading._start_new_thread = get_original('thread', 'start_new_thread')
    threading._allocate_lock = get_original('thread', 'allocate_lock')
    threading._get_ident = get_original('thread', 'get_ident')
    threading.ThreadError = get_original('thread', 'error')
    try:
        threading._CRLock = get_original('thread', 'RLock')
        threading.TIMEOUT_MAX = get_original('thread', 'TIMEOUT_MAX')
    except AttributeError:
        pass
    return threading

_threading = _recreate_threading()


class GeventEnv(BaseEnv):
    """Implementation of gevent-compatible environment."""

    @cached_property
    def socket(self):
        return get_real_module('socket')

    @cached_property
    def _start_real_thread(self):
        return get_original('thread', 'start_new_thread')

    def start_real_thread(self, func, args=None, kwargs=None):
        return self._start_real_thread(func, args or (), kwargs or {})

    @cached_property
    def get_real_ident(self):
        return get_original('thread', 'get_ident')

    @property
    def Event(self):
        return Event

    @property
    def RealEvent(self):
        """Event that ignore monkey patching."""
        return _threading.Event

    @property
    def RLock(self):
        return RLock

    def start_thread(self, func, args=None, kwargs=None):
        return start_new_thread(func, args or (), kwargs or {})

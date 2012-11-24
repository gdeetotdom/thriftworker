from __future__ import absolute_import

from abc import ABCMeta, abstractproperty, abstractmethod

from thriftworker.utils.imports import get_real_module
from thriftworker.utils.decorators import cached_property

from .mutex import Mutex


class BaseEnv(object):

    __metaclass__ = ABCMeta

    @cached_property
    def _thread(self):
        return get_real_module('thread')

    @cached_property
    def _time(self):
        return get_real_module('time')

    @cached_property
    def socket(self):
        return get_real_module('socket')

    @cached_property
    def _threading(self):
        _thread = self._thread
        _time = self._time
        threading = get_real_module('threading')
        threading._time = _time.time
        threading._sleep = _time.sleep
        threading._start_new_thread = _thread.start_new_thread
        threading._allocate_lock = _thread.allocate_lock
        threading._get_ident = _thread.get_ident
        threading.ThreadError = _thread.error
        try:
            threading._CRLock = _thread.RLock
            threading.TIMEOUT_MAX = _thread.TIMEOUT_MAX
        except AttributeError:
            pass
        return threading

    def start_real_thread(self, func, args=None, kwargs=None):
        """Start new OS thread regardless of any monkey patching."""
        return self._thread.start_new_thread(func, tuple(args or []),
                                             kwargs or {})

    def get_real_ident(self):
        """Get identification of current thread regardless of any monkey
        patching.

        """
        return self._thread.get_ident()

    @property
    def RealEvent(self):
        """Event that ignore monkey patching."""
        return self._threading.Event

    @abstractproperty
    def Event(self):
        raise NotImplementedError()

    @abstractproperty
    def RLock(self):
        raise NotImplementedError()

    @abstractmethod
    def start_thread(self, func, args=None, kwargs=None):
        raise NotImplementedError()

    @property
    def Mutex(self):
        return Mutex

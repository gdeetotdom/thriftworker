from __future__ import absolute_import

from abc import ABCMeta, abstractproperty, abstractmethod


class BaseEnv(object):
    """Provided methods and property that should be used to work properly with
    current environment, for example, monkey patched Python stdlib by gevent.

    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def start_real_thread(self, func, args=None, kwargs=None):
        raise NotImplementedError()

    @abstractmethod
    def get_real_ident(self):
        raise NotImplementedError()

    @abstractproperty
    def RealEvent(self):
        raise NotImplementedError()

    @abstractproperty
    def socket(self):
        raise NotImplementedError()

    @abstractproperty
    def Event(self):
        raise NotImplementedError()

    @abstractproperty
    def RLock(self):
        raise NotImplementedError()

    @abstractmethod
    def start_thread(self, func, args=None, kwargs=None):
        raise NotImplementedError()

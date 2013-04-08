from __future__ import absolute_import

from pyuv.thread import Condition, Mutex


class Event(object):

    def __init__(self):
        self._lock = Mutex()
        self._cond = Condition()
        self._flag = False

    def isSet(self):
        return self._flag
    is_set = isSet

    def set(self):
        with self._lock:
            self._flag = True
            self._cond.broadcast()

    def clear(self):
        with self._lock:
            self._flag = False

    def wait(self, timeout=None):
        with self._lock:
            if not self._flag:
                if timeout is None:
                    self._cond.wait(self._lock)
                else:
                    self._cond.timedwait(self._lock, timeout)
            return self._flag

from __future__ import absolute_import

import os
import itertools

try:
    from _billiard import SemLock
except ImportError:
    try:
        from _multiprocessing import SemLock
    except ImportError:
        semaphore_exists = False
else:
    semaphore_exists = True

try:
    sem_unlink = SemLock.sem_unlink
except AttributeError:
    sem_unlink = None

from thriftworker.utils.decorators import cached_property

__all__ = ['Mutex']


class Mutex(object):
    _counter = itertools.count()

    def __init__(self):
        if not semaphore_exists:
            raise RuntimeError('SemLock not avalaible!')

    @cached_property
    def _semlock(self):
        kind, value, maxvalue = 1, 1, 1
        if sem_unlink:
            sl = SemLock(kind, value, maxvalue,
                         self._make_name(), False)
        else:
            sl = SemLock(kind, value, maxvalue)
        return sl

    @property
    def value(self):
        return self._semlock._get_value()

    def __del__(self):
        if sem_unlink is not None and '_semlock' in vars(self):
            sem_unlink(self._semlock.name)

    def __enter__(self):
        return self._semlock.__enter__()

    def __exit__(self, *args):
        return self._semlock.__exit__(*args)

    def __getstate__(self):
        sl = self._semlock
        state = (sl.handle, sl.kind, sl.maxvalue)
        try:
            state += (sl.name, )
        except AttributeError:
            pass
        return state

    def __setstate__(self, state):
        self._semlock = SemLock._rebuild(*state)

    @classmethod
    def _make_name(cls):
        return '/%s-%s-%s' % ('_thriftpool',
                              os.getpid(), next(cls._counter))

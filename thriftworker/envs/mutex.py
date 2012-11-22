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

from thriftworker.utils.finalize import Finalize
from thriftworker.utils.decorators import cached_property

__all__ = ['Mutex']


class Mutex(object):
    _counter = itertools.count()

    def __init__(self):
        self._finalize = None
        super(Mutex, self).__init__()

    @cached_property
    def _semlock(self):
        if not semaphore_exists:
            raise RuntimeError('SemLock not available!')
        kind, value, maxvalue = 1, 1, 1
        if sem_unlink:
            sl = SemLock(kind, value, maxvalue,
                         self._make_name(), False)
            self._finalize = Finalize(self, sem_unlink, (sl.name,),
                                      exitpriority=0)
        else:
            sl = SemLock(kind, value, maxvalue)
        return sl

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

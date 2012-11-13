"""Execute some function in loop and wait for answer."""
from __future__ import absolute_import

import sys
from threading import Event
from functools import wraps

from pyuv import Async

from .threads import get_ident


class in_loop(object):
    """Schedule execution of given function in main event loop. Wait for
    function execution.

    """

    def __init__(self, func=None, timeout=None):
        self.__func = func
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__timeout = timeout or 5.0

    def __create(self, obj):
        method = self.__func.__get__(obj)

        @wraps(self.__func)
        def inner_decorator(*args, **kwargs):
            event = Event()
            d = {'result': None, 'tb': None}

            def inner_callback(async_handle):
                async.close()
                try:
                    d['result'] = method(*args, **kwargs)
                except Exception as exc:
                    # Save traceback here.
                    d['result'] = exc
                    d['tb'] = sys.exc_info()[2]
                finally:
                    event.set()

            async = Async(obj.loop, inner_callback)
            async.send()
            try:
                if not event.wait(self.__timeout):
                    raise Exception('Timeout happened when calling method'
                                    ' {0!r} of {1!r}'.format(self.__name__, obj))
            finally:
                event.clear()
                if not async.closed:
                    async.close()

            result, tb = d['result'], d['tb']
            if isinstance(result, Exception):
                # Restore traceback.
                raise result.__class__, result, tb
            else:
                return result

        if obj.loop.ident == get_ident():
            # Don't block main loop.
            return method
        else:
            return inner_decorator

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            value = obj.__dict__[self.__name__] = self.__create(obj)
            return value

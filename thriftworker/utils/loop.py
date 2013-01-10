"""Execute some function in loop and wait for answer."""
from __future__ import absolute_import

import sys
from functools import wraps

import six

from thriftworker.state import current_app


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

        def delayed_method(*args, **kwargs):
            """Execute method in loop but wait for it."""
            event = current_app.env.RealEvent()
            d = {'result': None, 'exception': None}

            def inner_callback():
                try:
                    d['result'] = method(*args, **kwargs)
                except:
                    d['exception'] = sys.exc_info()
                finally:
                    event.set()

            current_app.hub.callback(inner_callback)
            try:
                if not event.wait(self.__timeout):
                    raise Exception('Timeout happened when calling method'
                                    ' {0!r} of {1!r}'
                                    .format(self.__name__, obj))
            finally:
                event.clear()

            if d['exception'] is not None:
                exc_type, exc, tb = d['exception']
                six.reraise(exc_type, exc, tb)
            else:
                return d['result']

        @wraps(self.__func)
        def inner_decorator(*args, **kwargs):
            """Detect current thread and use appropriate method to avoid
            loop blocking.

            """
            try:
                ident = current_app.loop.ident
            except AttributeError:
                raise RuntimeError('Loop not started')

            if ident == current_app.env.get_real_ident():
                # Don't block main loop.
                return method(*args, **kwargs)
            else:
                return delayed_method(*args, **kwargs)

        return inner_decorator

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            value = obj.__dict__[self.__name__] = self.__create(obj)
            return value

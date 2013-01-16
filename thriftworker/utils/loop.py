"""Execute some function in loop and wait for answer."""
from __future__ import absolute_import

import sys
from functools import wraps

import six

from thriftworker.state import current_app


def loop_delegate(*args, **options):
    """Run given function in loop.

    :param timeout: how many seconds we should wait before raise exception?

    """

    def inner_loop_delegate(timeout=None):
        timeout = timeout or 5.0

        def inner_decorator(func):

            def execute_in_loop(*args, **kwargs):
                event = current_app.env.RealEvent()
                d = {'result': None, 'exception': None}

                def inner_callback():
                    try:
                        d['result'] = func(*args, **kwargs)
                    except:
                        d['exception'] = sys.exc_info()
                    finally:
                        event.set()

                current_app.hub.callback(inner_callback)
                try:
                    if not event.wait(timeout):
                        raise Exception('Timeout happened when calling {0!r}'
                                        .format(func))
                finally:
                    event.clear()

                if d['exception'] is not None:
                    exc_type, exc, tb = d['exception']
                    six.reraise(exc_type, exc, tb)
                else:
                    return d['result']

            @wraps(func)
            def inner_wrapper(*args, **kwargs):
                """Detect current thread and use appropriate method to avoid
                loop blocking.

                """
                try:
                    ident = current_app.loop.ident
                except AttributeError:
                    raise RuntimeError('Loop not started')

                if ident == current_app.env.get_real_ident():
                    # Don't block main loop.
                    return func(*args, **kwargs)
                else:
                    return execute_in_loop(*args, **kwargs)

            return inner_wrapper

        return inner_decorator

    if len(args) == 1 and callable(args[0]):
        return inner_loop_delegate(**options)(*args)
    return inner_loop_delegate(**options)


class in_loop(object):
    """Schedule execution of given function in main event loop. Wait for
    function execution.

    """

    def __init__(self, func=None, timeout=None):
        self.__func = func
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__timeout = timeout

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            method = self.__func.__get__(obj)
            value = obj.__dict__[self.__name__] = \
                loop_delegate(timeout=self.__timeout)(method)
            return value

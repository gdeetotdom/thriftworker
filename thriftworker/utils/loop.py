"""Execute some function in loop and wait for answer."""
from __future__ import absolute_import

import sys
from functools import wraps, partial
from thread import get_ident
from threading import Event

import six

from thriftworker.state import current_app

#: Specify default timeout for delegation decorators.
DELEGATION_TIMEOUT = 5.0


def _create_decorator(decorator, *args, **options):
    """Execute given decorator if and only if function executed not in loop.

    """

    def inner_create_decorator(**options):

        def inner_decorator(func):

            @wraps(func)
            def inner_wrapper(*args, **kwargs):
                """Detect current thread and use appropriate method to avoid
                loop blocking.

                """
                try:
                    ident = current_app.loop.ident
                except AttributeError:
                    raise RuntimeError('Loop not started')

                if ident == get_ident():
                    # Don't block main loop.
                    return func(*args, **kwargs)
                else:
                    return decorator(func, options, *args, **kwargs)

            return inner_wrapper

        return inner_decorator

    if len(args) == 1 and callable(args[0]):
        return inner_create_decorator(**options)(*args)
    return inner_create_decorator(**options)


class Container(object):
    """Mutable that store result."""

    __slots__ = ['_result', '_exception', '_event', 'timeout', 'func']

    def __init__(self, func, timeout=None):
        self.func = func
        self.timeout = timeout
        self._result = None
        self._exception = None
        self._event = Event()

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, value):
        assert isinstance(value, (Exception, tuple))
        self._exception = value
        self._event.set()

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, value):
        self._result = value
        self._event.set()

    def from_greenlet(self, g):
        if g.exception is None:
            self.result = g.value
        else:
            print g.exception
            self.exception = g.exception

    def dispatch(self):
        try:
            if not self._event.wait(self.timeout):
                raise RuntimeError('Timeout happened waiting for {0!r}'
                                   .format(self.func))
        finally:
            self._event.clear()
        exception = self._exception
        if exception is None:
            return self._result
        elif isinstance(exception, tuple):
            exc_type, exc, tb = exception
            six.reraise(exc_type, exc, tb)
        else:
            raise exception


def _loop_delegate(func, options, *args, **kwargs):
    """Run given function in loop.

    :param timeout: how many seconds we should wait before raise exception?

    """
    container = Container(func,
                          timeout=options.get('timeout') or DELEGATION_TIMEOUT)

    def inner_callback():
        try:
            container.result = func(*args, **kwargs)
        except:
            container.exception = sys.exc_info()

    current_app.hub.callback(inner_callback)
    return container.dispatch()

loop_delegate = partial(_create_decorator, _loop_delegate)


def _greenlet_delegate(func, options, *args, **kwargs):
    """Run given function in greenlet.

    :param timeout: how many seconds we should wait before raise exception?

    """
    container = Container(func,
                          timeout=options.get('timeout') or DELEGATION_TIMEOUT)

    def inner_callback():
        g = current_app.hub.spawn(func, *args, **kwargs)
        g.rawlink(container.from_greenlet)

    current_app.hub.callback(inner_callback)
    return container.dispatch()

greenlet_delegate = partial(_create_decorator, _greenlet_delegate)


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

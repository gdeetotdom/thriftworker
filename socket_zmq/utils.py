"""Some useful tools.

This file was copied and adapted from celery.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys
from functools import wraps

from .constants import DEFAULT_ENV, GEVENT_ENV

__all__ = ['cached_property', 'SubclassMixin', 'in_loop', 'spawn',
           'detect_environment', 'Event']

_realthread = None
_environment = None


class cached_property(object):
    """Property descriptor that caches the return value
    of the get function.

    *Examples*

    .. code-block:: python

        @cached_property
        def connection(self):
            return Connection()

        @connection.setter  # Prepares stored value
        def connection(self, value):
            if value is None:
                raise TypeError('Connection must be a connection')
            return value

        @connection.deleter
        def connection(self, value):
            # Additional action to do at del(self.attr)
            if value is not None:
                print('Connection %r deleted' % (value, ))

    """

    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.__get = fget
        self.__set = fset
        self.__del = fdel
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            value = obj.__dict__[self.__name__] = self.__get(obj)
            return value

    def __set__(self, obj, value):
        if obj is None:
            return self
        if self.__set is not None:
            value = self.__set(obj, value)
        obj.__dict__[self.__name__] = value

    def __delete__(self, obj):
        if obj is None:
            return self
        try:
            value = obj.__dict__.pop(self.__name__)
        except KeyError:
            pass
        else:
            if self.__del is not None:
                self.__del(obj, value)

    def setter(self, fset):
        return self.__class__(self.__get, fset, self.__del)

    def deleter(self, fdel):
        return self.__class__(self.__get, self.__set, fdel)


class SubclassMixin(object):

    def subclass_with_self(self, Class, name=None, attribute='app',
            reverse=None, **kw):
        """Subclass an app-compatible class by setting its app attribute
        to be this app instance.

        App-compatible means that the class has a class attribute that
        provides the default app it should use, e.g.
        ``class Foo: app = None``.

        :param Class: The app-compatible class to subclass.
        :keyword name: Custom name for the target class.
        :keyword attribute: Name of the attribute holding the app,
                            default is 'app'.

        """
        reverse = reverse if reverse else Class.__name__

        attrs = dict({attribute: self}, __module__=Class.__module__,
                     __doc__=Class.__doc__, **kw)

        return type(name or Class.__name__, (Class,), attrs)


def get_realthread():
    """Get the real Python thread module, regardless of any monkeypatching"""
    global _realthread
    if _realthread:
        return _realthread

    import imp
    fp, pathname, description = imp.find_module('thread')
    try:
        _realthread = imp.load_module('realthread', fp, pathname, description)
        return _realthread
    finally:
        if fp:
            fp.close()


def spawn(func, *args, **kwargs):
    """Takes a function and spawns it as a daemon thread using the
    real OS thread regardless of monkey patching.

    """
    return get_realthread().start_new_thread(func, args, kwargs)


def _detect_environment():

    # -gevent-
    if 'gevent' in sys.modules:
        try:
            from gevent import socket as _gsocket
            import socket

            if socket.socket is _gsocket.socket:
                return GEVENT_ENV
        except ImportError:
            pass

    return DEFAULT_ENV


def detect_environment():
    global _environment
    if _environment is None:
        _environment = _detect_environment()
    return _environment


if detect_environment() == GEVENT_ENV:
    from gevent.event import Event

else:
    from threading import Event


def in_loop(func):
    """Schedule execution of given function in main event loop. Result of
    function ignored. Wait for function execution.

    :param func: Given callable.

    """

    @wraps(func)
    def inner_decorator(self, *args, **kwargs):
        event = Event()

        def inner_callback(watcher, revents):
            func(self, *args, **kwargs)
            async.stop()
            event.set()

        async = self.loop.async(inner_callback)
        async.start()
        async.send()
        event.wait()

    return inner_decorator

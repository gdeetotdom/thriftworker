"""Some useful tools.

This file was copied and adapted from celery.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys
import itertools
from functools import wraps

from pyuv import Async

from .constants import DEFAULT_ENV, GEVENT_ENV

__all__ = ['cached_property', 'SubclassMixin', 'in_loop', 'spawn',
           'detect_environment', 'Event', 'LoopMixin']

_realthread = None
_environment = None


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

        return inner_decorator

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            value = obj.__dict__[self.__name__] = self.__create(obj)
            return value


def get_port_from_range(name, range_start, range_end):
    """Get random port from given range [range_start, range_end]."""
    # Detect port for this service.
    hashed_port = abs(hash(name) & 0xffff)
    # Ensure that detected port is in pool.
    while hashed_port > range_start:
        hashed_port = hashed_port - range_start
    # If port is greater than allowed.
    while range_end < hashed_port + range_start:
        hashed_port = hashed_port // 2
    return hashed_port + range_start


def get_addresses_from_pool(name, address, port_range):
    """Get addresses from pool."""
    host, port = address
    if isinstance(port, int):
        ports = (port or 0,)
    elif isinstance(port, basestring) or port is None:
        if port_range is None and port is not None:
            raise ValueError('Port range not specified')
        elif port == 'random':
            ports = (0,)
        elif port is None or port == 'pool':
            range_start, range_end = port_range[0], port_range[1]
            default_port = get_port_from_range(name, range_start, range_end)
            chains = [(default_port,),
                      xrange(default_port, range_end),
                      xrange(range_start, default_port)]
            ports = itertools.chain.from_iterable(chains)
        elif port.isdigit():
            ports = (int(port),)
        else:
            raise ValueError('Unknown port {0!r}'.format(port))
    else:
        raise ValueError('Unknown address {0!r}'.format(address))
    for port in ports:
        yield (host, port)

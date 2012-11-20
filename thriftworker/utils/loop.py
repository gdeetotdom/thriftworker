"""Execute some function in loop and wait for answer."""
from __future__ import absolute_import

import sys
from functools import wraps

from pyuv import Prepare, Idle

from thriftworker.state import current_app

from .decorators import cached_property


class in_loop(object):
    """Schedule execution of given function in main event loop. Wait for
    function execution.

    """

    def __init__(self, func=None, timeout=None):
        self.__func = func
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__timeout = timeout or 5.0

    @cached_property
    def _spinner(self):
        return Idle(current_app.loop)

    @cached_property
    def _dispatcher(self):
        return Prepare(current_app.loop)

    @cached_property
    def _is_done(self):
        return current_app.env.RealEvent()

    def __create(self, obj):
        method = self.__func.__get__(obj)

        @wraps(self.__func)
        def inner_decorator(*args, **kwargs):
            event = self._is_done
            d = {'result': None, 'tb': None}

            def inner_callback(handle):
                handle.stop()
                try:
                    d['result'] = method(*args, **kwargs)
                except Exception as exc:
                    # Save traceback here.
                    d['result'] = exc
                    d['tb'] = sys.exc_info()[2]
                finally:
                    event.set()

            self._spinner.start(lambda h: h.stop())
            self._dispatcher.start(inner_callback)
            current_app.loop_container.wakeup()
            try:
                if not event.wait(self.__timeout):
                    raise Exception('Timeout happened when calling method'
                                    ' {0!r} of {1!r}'.format(self.__name__, obj))
            finally:
                event.clear()

            result, tb = d['result'], d['tb']
            if isinstance(result, Exception):
                # Restore traceback.
                raise result.__class__, result, tb
            else:
                return result

        try:
            ident = current_app.loop.ident
        except AttributeError:
            raise RuntimeError('Loop not started')

        if ident == current_app.env.get_real_ident():
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

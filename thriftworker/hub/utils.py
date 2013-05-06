from __future__ import absolute_import

from six import PY3
from pyuv import Timer

from ..state import current_app


class _NONE(object):
    "A special thingy you must never pass to any of API"
    __slots__ = []

    def __repr__(self):
        return '<_NONE>'

_NONE = _NONE()


if PY3:
    _meth_self = "__self__"
else:
    _meth_self = "im_self"


def getfuncname(func):
    if not hasattr(func, _meth_self):
        try:
            funcname = func.__name__
        except AttributeError:
            pass
        else:
            if funcname != '<lambda>':
                return funcname
    return repr(func)


def sleep(seconds=0):
    """Put the current greenlet to sleep for at least *seconds*.

    *seconds* may be specified as an integer, or a float if fractional seconds
    are desired.

    """
    hub = current_app.hub
    loop = current_app.loop
    if seconds <= 0:
        waiter = hub.Waiter()
        loop.callback(waiter.switch)
        waiter.get()
    else:
        hub.wait(Timer(loop), seconds, False)

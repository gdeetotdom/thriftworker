from __future__ import absolute_import

import sys

from greenlet import getcurrent

from .utils import _NONE


class Waiter(object):
    """A low level communication utility for greenlets.

    Wrapper around greenlet's ``switch()`` and ``throw()`` calls that makes
    them somewhat safer:

    * switching will occur only if the waiting greenlet is executing
      :meth:`get` method currently;
    * any error raised in the greenlet is handled inside :meth:`switch`
      and :meth:`throw`
    * if :meth:`switch`/:meth:`throw` is called before the receiver calls
      :meth:`get`, then :class:`Waiter` will store the value/exception.
      The following :meth:`get` will return the value/raise the exception.

    The :meth:`switch` and :meth:`throw` methods must only be called from
    the :class:`Hub` greenlet. The :meth:`get` method must be called from
    a greenlet other than :class:`Hub`.

    """

    __slots__ = ['hub', 'greenlet', 'value', '_exception']

    def __init__(self, hub):
        self.hub = hub
        self.greenlet = None
        self.value = None
        self._exception = _NONE

    def clear(self):
        self.greenlet = None
        self.value = None
        self._exception = _NONE

    def __str__(self):
        if self._exception is _NONE:
            return '<%s greenlet=%s>' % (type(self).__name__, self.greenlet)
        elif self._exception is None:
            return '<%s greenlet=%s value=%r>' % (type(self).__name__, self.greenlet, self.value)
        else:
            return '<%s greenlet=%s exc_info=%r>' % (type(self).__name__, self.greenlet, self.exc_info)

    def ready(self):
        """Return true if and only if it holds a value or an exception"""
        return self._exception is not _NONE

    def successful(self):
        """Return true if and only if it is ready and holds a value"""
        return self._exception is None

    @property
    def exc_info(self):
        "Holds the exception info passed to :meth:`throw` if :meth:`throw` was called. Otherwise ``None``."
        if self._exception is not _NONE:
            return self._exception

    def switch(self, value=None):
        """Switch to the greenlet if one's available. Otherwise store the value."""
        greenlet = self.greenlet
        if greenlet is None:
            self.value = value
            self._exception = None
        else:
            assert getcurrent() is self.hub._greenlet, "Can only use Waiter.switch method from the Hub greenlet"
            switch = greenlet.switch
            try:
                switch(value)
            except:
                self.hub.handle_error(*sys.exc_info())

    def switch_args(self, *args):
        return self.switch(args)

    def throw(self, *throw_args):
        """Switch to the greenlet with the exception. If there's no greenlet, store the exception."""
        greenlet = self.greenlet
        if greenlet is None:
            self._exception = throw_args
        else:
            assert getcurrent() is self.hub._greenlet, "Can only use Waiter.switch method from the Hub greenlet"
            throw = greenlet.throw
            try:
                throw(*throw_args)
            except:
                self.hub.handle_error(*sys.exc_info())

    def get(self):
        """If a value/an exception is stored, return/raise it. Otherwise until switch() or throw() is called."""
        if self._exception is not _NONE:
            if self._exception is None:
                return self.value
            else:
                getcurrent().throw(*self._exception)
        else:
            assert self.greenlet is None, 'This Waiter is already used by %r' % (self.greenlet, )
            self.greenlet = getcurrent()
            try:
                return self.hub.switch()
            finally:
                self.greenlet = None

    def __call__(self, source):
        if source.exception is None:
            self.switch(source.value)
        else:
            self.throw(source.exception)

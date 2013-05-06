from __future__ import absolute_import

import sys
import logging
from collections import deque

from six import PY3
from greenlet import greenlet, getcurrent, GreenletExit

from .utils import _NONE, getfuncname


def _kill(greenlet, exception, waiter):
    try:
        greenlet.throw(exception)
    except Exception as exc:
        logging.exception(exc)
    waiter.switch()


class _dummy_event(object):

    def stop(self):
        pass

_dummy_event = _dummy_event()


class Greenlet(greenlet):
    """A light-weight cooperatively-scheduled execution unit."""

    def __init__(self, hub, run=None, *args, **kwargs):
        greenlet.__init__(self, parent=hub._greenlet)
        if run is not None:
            self._run = run
        self.hub = hub
        self.args = args
        self.kwargs = kwargs
        self._links = deque()
        self.value = None
        self._exception = _NONE
        self._notifier = None
        self._start_event = None

    if PY3:
        def __bool__(self):
            return self._start_event is not None and self._exception is _NONE
    else:
        def __nonzero__(self):
            return self._start_event is not None and self._exception is _NONE

    def ready(self):
        """Return true if and only if the greenlet has finished execution."""
        return self.dead or self._exception is not _NONE

    def successful(self):
        """Return true if and only if the greenlet has finished execution
        successfully, that is, without raising an error.
        """
        return self._exception is None

    def __repr__(self):
        classname = self.__class__.__name__
        result = '<%s at %s' % (classname, hex(id(self)))
        formatted = self._formatinfo()
        if formatted:
            result += ': ' + formatted
        return result + '>'

    def _formatinfo(self):
        try:
            return self._formatted_info
        except AttributeError:
            pass
        try:
            result = getfuncname(self.__dict__['_run'])
        except Exception:
            pass
        else:
            args = []
            if self.args:
                args = [repr(x)[:50] for x in self.args]
            if self.kwargs:
                args.extend(['%s=%s' % (key, repr(value)[:50]) for (key, value) in self.kwargs.items()])
            if args:
                result += '(' + ', '.join(args) + ')'
            # it is important to save the result here, because once the greenlet exits '_run' attribute will be removed
            self._formatted_info = result
            return result
        return ''

    @property
    def exception(self):
        """Holds the exception instance raised by the function if the greenlet
        has finished with an error. Otherwise ``None``.

        """
        if self._exception is not _NONE:
            return self._exception

    def throw(self, *args):
        """Immediately switch into the greenlet and raise an exception in it.

        Should only be called from the HUB, otherwise the current greenlet is left unscheduled forever.
        To raise an exception in a safely manner from any greenlet, use :meth:`kill`.

        If a greenlet was started but never switched to yet, then also
        a) cancel the event that will start it
        b) fire the notifications as if an exception was raised in a greenlet

        """
        try:
            greenlet.throw(self, *args)
        finally:
            if self._exception is _NONE and self.dead:
                # the greenlet was never switched to before and it will never be, _report_error was not called
                # the result was not set and the links weren't notified. let's do it here.
                # checking that self.dead is true is essential, because throw() does not necessarily kill the greenlet
                # (if the exception raised by throw() is caught somewhere inside the greenlet).
                if len(args) == 1:
                    arg = args[0]
                    #if isinstance(arg, type):
                    if type(arg) is type(Exception):
                        args = (arg, arg(), None)
                    else:
                        args = (type(arg), arg, None)
                elif not args:
                    args = (GreenletExit, GreenletExit(), None)
                self._report_error(args)

    def start(self):
        """Schedule the greenlet to run in this loop iteration"""
        if self._start_event is None:
            self._start_event = self.hub.callback(self.switch)

    def kill(self, exception=GreenletExit, block=True):
        """Raise the exception in the greenlet.

        If block is ``True`` (the default), wait until the greenlet dies.
        If block is ``False``, the current greenlet is not unscheduled.

        The function always returns ``None`` and never raises an error.

        """
        # XXX this function should not switch out if greenlet is not started but it does
        # XXX fix it (will have to override 'dead' property of greenlet.greenlet)
        if self._start_event is None:
            self._start_event = _dummy_event
        else:
            self._start_event.stop()
        if not self.dead:
            waiter = self.hub.Waiter()
            self.hub.callback(_kill, self, exception, waiter)
            if block:
                waiter.get()
                self.join()
        # it should be OK to use kill() in finally or kill a greenlet from more
        # than one place; thus it should not raise when the greenlet is already
        # killed (= not started)

    def get(self):
        """Return the result the greenlet has returned or re-raise the
        exception it has raised.

        """
        if self.ready():
            if self.successful():
                return self.value
            else:
                raise self._exception
        switch = getcurrent().switch
        self.rawlink(switch)
        try:
            result = self.parent.switch()
            assert result is self, 'Invalid switch into Greenlet.get(): %r' % (result, )
        except:
            # unlinking in 'except' instead of finally is an optimization:
            # if switch occurred normally then link was already removed in _notify_links
            # and there's no need to touch the links set.
            # Note, however, that if "Invalid switch" assert was removed and invalid switch
            # did happen, the link would remain, causing another invalid switch later in this greenlet.
            self.unlink(switch)
            raise
        if self.ready():
            if self.successful():
                return self.value
            else:
                raise self._exception

    def join(self):
        """Wait until the greenlet finishes. Return ``None`` regardless."""
        if self.ready():
            return
        else:
            switch = getcurrent().switch
            self.rawlink(switch)
            try:
                result = self.parent.switch()
                assert result is self, 'Invalid switch into Greenlet.join(): %r' % (result, )
            except:
                self.unlink(switch)
                raise

    def _report_result(self, result):
        self._exception = None
        self.value = result
        if self._links and not self._notifier:
            self._notifier = self.hub.callback(self._notify_links)

    def _report_error(self, exc_info):
        exception = exc_info[1]
        if isinstance(exception, GreenletExit):
            self._report_result(exception)
            return
        self._exception = exception

        if self._links and not self._notifier:
            self._notifier = self.hub.callback(self._notify_links)

        self.hub.handle_error(*exc_info)

    def run(self):
        try:
            if self._start_event is None:
                self._start_event = _dummy_event
            else:
                self._start_event.stop()
            try:
                result = self._run(*self.args, **self.kwargs)
            except:
                self._report_error(sys.exc_info())
                return
            self._report_result(result)
        finally:
            self.__dict__.pop('_run', None)
            self.__dict__.pop('args', None)
            self.__dict__.pop('kwargs', None)

    def rawlink(self, callback):
        """Register a callable to be executed when the greenlet finishes the execution.

        WARNING: the callable will be called in the HUB greenlet.
        """
        if not callable(callback):
            raise TypeError('Expected callable: %r' % (callback, ))
        self._links.append(callback)
        if self.ready() and self._links and not self._notifier:
            self._notifier = self.hub.callback(self._notify_links)

    def unlink(self, receiver):
        """Remove the receiver set by :meth:`link` or :meth:`rawlink`"""
        try:
            self._links.remove(receiver)
        except ValueError:
            pass

    def _notify_links(self):
        while self._links:
            link = self._links.popleft()
            try:
                link(self)
            except:
                self.hub.handle_error(*sys.exc_info())

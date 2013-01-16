"""Container for event loop. Prevent loop to exit when no watchers exists."""
from __future__ import absolute_import

import sys
import logging
from collections import deque
from functools import partial

from six import PY3
from greenlet import greenlet, getcurrent, GreenletExit
from pyuv import Async, Prepare, Timer
from pyuv.error import HandleError

from .state import current_app
from .utils.loop import in_loop
from .utils.mixin import LoopMixin
from .utils.decorators import cached_property

logger = logging.getLogger(__name__)


def _kill(greenlet, exception, waiter):
    try:
        greenlet.throw(exception)
    except Exception as exc:
        logger.exception(exc)
    waiter.switch()


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


class _NONE(object):
    "A special thingy you must never pass to any of API"
    __slots__ = []

    def __repr__(self):
        return '<_NONE>'

_NONE = _NONE()


class _dummy_event(object):

    def stop(self):
        pass


_dummy_event = _dummy_event()


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


class LoopGreenlet(greenlet):
    """Greenlet that execute main loop."""

    def __init__(self, loop):
        self.loop = loop
        greenlet.__init__(self)

    def run(self):
        try:
            self.loop.run()
        except Exception as exc:
            logger.exception(exc)

    def switch(self):
        switch_out = getattr(getcurrent(), 'switch_out', None)
        if switch_out is not None:
            switch_out()
        return greenlet.switch(self)

    def switch_out(self):
        raise AssertionError('Impossible to call blocking function in the'
                             ' event loop callback')


class Callback(object):
    """Callback event return by :meth:`Hub.callback` that store function
    arguments and function itself.

    """

    __slots__ = ['run', 'args', 'kwargs', 'alive']

    def __init__(self, run, *args, **kwargs):
        self.run = run
        self.args = args
        self.kwargs = kwargs
        self.alive = True

    def __call__(self):
        if not self.alive:
            return
        try:
            return self.run(*self.args, **self.kwargs)
        except Exception as exc:
            logger.exception(exc)
        finally:
            self.stop()

    if PY3:
        def __bool__(self):
            return self.alive
    else:
        def __nonzero__(self):
            return self.alive

    def stop(self):
        """Prevent callback execution if it's not already called."""
        self.alive = False
        self.run = self.args = self.kwargs = None


class Hub(LoopMixin):
    """Container for event loop."""

    app = None

    def __init__(self):
        self._outgoing = deque()
        self.Waiter = partial(Waiter, self)
        self.Greenlet = partial(Greenlet, self)

    @cached_property
    def _started(self):
        """Set when loop started."""
        return self.app.env.RealEvent()

    @cached_property
    def _stopped(self):
        """Set when loop stopped."""
        return self.app.env.RealEvent()

    @cached_property
    def _greenlet(self):
        """Greenlet in which we run loop."""
        return LoopGreenlet(self.loop)

    @cached_property
    def _guard(self):
        """Prevent loop exit there is nothing to do."""
        return Async(self.loop, lambda h: None)

    def wakeup(self):
        """Send wakeup signal to loop."""
        assert self._guard is not None, 'loop not started'
        try:
            self._guard.send()
        except HandleError:
            pass  # pragma: no cover

    @cached_property
    def _callback_handle(self):
        """Handle that should run functions in loop thread."""
        return Prepare(self.loop)

    def _before_iteration(self, handle):
        """Should be used to run functions in loop's thread."""
        outgoing = self._outgoing
        while True:
            try:
                callback = outgoing.popleft()
            except IndexError:
                break
            else:
                callback()

    def callback(self, fn, *args, **kwargs):
        """Enqueue function execution to loop. Return :class:`Callback`
        instance.

        """
        cb = Callback(fn, *args, **kwargs)
        self._outgoing.append(cb)
        self.wakeup()
        return cb

    def handle_error(self, exc_type, value, traceback):
        """Log in-loop errors with our logger."""
        logger.exception(value, exc_info=(exc_type, value, traceback))

    def _setup_loop(self, loop):
        loop.ident = self.app.env.get_real_ident()
        loop.excepthook = self.handle_error
        self._callback_handle.start(self._before_iteration)

    def _teardown_loop(self, loop):
        loop.excepthook = None

    def _run(self):
        """Run event loop."""
        loop = self.loop
        started = self._started
        stopped = self._stopped
        greenlet = self._greenlet
        self._setup_loop(loop)
        logger.debug('Loop %s started...', hex(id(loop)))
        started.set()
        try:
            greenlet.switch()
        finally:
            logger.debug('Loop %s stopped...', hex(id(loop)))
            stopped.set()
            self._teardown_loop(loop)
            del self._greenlet
            del self._guard
            del self._callback_handle

    @in_loop
    def _close_handlers(self):
        """Close all stale handlers."""
        def cb_handle(handle):
            if not getattr(handle, 'bypass', False) and not handle.closed:
                logger.debug('Close stale handle %r', handle)
                handle.close()
        self.loop.walk(cb_handle)

    def start(self):
        """Start event loop in separate thread."""
        # Cleanup events.
        self._started.clear()
        self._stopped.clear()
        # Prevent loop exit.
        self._guard.send()
        # Start loop in separate thread.
        self.app.env.start_real_thread(self._run)
        self._started.wait()

    def stop(self):
        """Stop event loop and wait until it exit."""
        self.wakeup()
        self._close_handlers()
        self._stopped.wait()

    def switch(self):
        """Switch into the loop's greenlet."""
        return self._greenlet.switch()

    def spawn(self, *args, **kwargs):
        """Return a new :class:`Greenlet` object, scheduled to start.

        The arguments are passed to :meth:`Greenlet.__init__`.

        """
        g = self.Greenlet(*args, **kwargs)
        g.start()
        return g

    def wait(self, watcher, *args, **kwargs):
        """Wait for given watcher."""
        waiter = self.Waiter()
        unique = object()
        watcher.start(lambda *args, **kwargs: partial(waiter.switch, unique),
                      *args, **kwargs)
        try:
            result = waiter.get()
            assert result is unique, \
                'Invalid switch into %s: %r (expected %r)' % \
                    (getcurrent(), result, unique)
        finally:
            watcher.stop()


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

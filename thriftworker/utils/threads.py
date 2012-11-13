"""Some function to work with system threads."""
from __future__ import absolute_import

import imp
import os
import sys
import traceback
from threading import Thread, Event

__all__ = ['spawn', 'DaemonThread']

_realthread = None


def get_realthread():
    """Get the real Python thread module, regardless of any monkeypatching"""
    global _realthread
    if _realthread:
        return _realthread

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


def get_ident():
    return get_realthread().get_ident()


class DaemonThread(Thread):

    def __init__(self, name=None, **kwargs):
        super(DaemonThread, self).__init__()
        self._is_stopped = Event()
        self.daemon = True
        self.name = name or self.__class__.__name__

    def body(self):
        raise NotImplementedError('subclass responsibility')

    def run(self):
        try:
            self.body()
        except Exception as exc:
            try:
                self.on_crash('{0!r} crashed: {1!r}', self.name, exc)
            finally:
                os._exit(1)  # exiting by normal means won't work
        finally:
            self._set_stopped()

    def _set_stopped(self):
        try:
            self._is_stopped.set()
        except TypeError:  # pragma: no cover
            # we lost the race at interpreter shutdown,
            # so gc collected built-in modules.
            pass

    def stop(self):
        """Graceful shutdown."""
        self._is_stopped.wait()
        if self.is_alive():
            self.join(1e100)

    def on_crash(self, msg, *fmt):
        sys.stderr.write(msg.format(*fmt) + '\n')
        exc_info = sys.exc_info()
        try:
            traceback.print_exception(exc_info[0], exc_info[1], exc_info[2],
                                      None, sys.stderr)
        finally:
            del(exc_info)


class LoopThread(DaemonThread):

    def __init__(self, name=None, **kwargs):
        super(LoopThread, self).__init__(name, **kwargs)
        self._is_shutdown = Event()

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def body(self):
        self.on_start()
        while not self._is_shutdown.is_set():
            self.loop()
        self.on_stop()

    def loop(self):
        raise NotImplementedError('subclass responsibility')

    def stop(self):
        """Graceful shutdown."""
        self._is_shutdown.set()
        super(LoopThread, self).stop()


class SimpleDaemonThread(DaemonThread):

    def __init__(self, target, name=None, **kwargs):
        self.target = target
        super(SimpleDaemonThread, self).__init__(name, **kwargs)

    def body(self):
        self.target()

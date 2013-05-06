from __future__ import absolute_import

from time import time, sleep
from contextlib import contextmanager
from unittest import TestCase as BaseTestCase
from threading import Event

from six import with_metaclass
from pyuv import Loop, Async, Idle

from thriftworker import state
from thriftworker.app import ThriftWorker
from thriftworker.utils.loop import greenlet_delegate

TIMEOUT = 5.0


@contextmanager
def start_stop_ctx(container):
    """Start container on enter and stop on exit to and from context."""
    container.start()
    try:
        yield container
    finally:
        container.stop()
        if hasattr(container, 'close'):
            container.close()


class TestCase(BaseTestCase):
    """Default test case that reset current application on each run."""

    def setUp(self):
        # reset current state before each run
        state.default_app = None


class CustomAppMixin(object):
    """Create application with custom loop."""

    def setUp(self):
        super(CustomAppMixin, self).setUp()
        loop = self.loop = Loop()
        self.app = ThriftWorker(loop=loop)

    def wait_for_predicate(self, func, timeout=TIMEOUT):
        tic = time()
        while func() and tic + timeout > time():
            sleep(0.1)


class StartStopLoopMixin(CustomAppMixin):
    """Ensure that hub will started be started before tests run."""

    def setUp(self):
        super(StartStopLoopMixin, self).setUp()
        hub = self.app.hub
        hub.start()
        self.addCleanup(hub.stop)

    def wakeup_loop(self):
        event = Event()
        handles = []

        def idle_cb(handle):
            event.set()
            handle.close()

        def async_cb(handle):
            idle = Idle(self.loop)
            idle.start(idle_cb)
            handles.append(idle)
            handle.close()

        async = Async(self.loop, async_cb)
        async.send()
        event.wait()


class GreenMeta(type):

    def __new__(meta, classname, bases, attrs):
        for key, value in attrs.items():
            if key.startswith('test') and callable(value):
                attrs.pop(key)
                attrs[key] = greenlet_delegate(value)
        return type.__new__(meta, classname, bases, attrs)


class GreenTest(CustomAppMixin, with_metaclass(GreenMeta, TestCase)):
    """Ensure that all methods will be executed in loop."""

    def setUp(self):
        super(GreenTest, self).setUp()
        hub = self.hub = self.app.hub
        hub.start()
        self.addCleanup(hub.stop)

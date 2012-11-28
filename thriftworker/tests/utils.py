from __future__ import absolute_import

from threading import Event
from pyuv import Loop, Async, Idle

from contextlib import contextmanager
from unittest import TestCase as BaseTestCase

from thriftworker import state
from thriftworker.app import ThriftWorker


@contextmanager
def start_stop_ctx(container):
    """Start container on enter and stop on exit to and from context."""
    container.start()
    try:
        yield container
    finally:
        container.stop()


class TestCase(BaseTestCase):

    def setUp(self):
        # reset current state before each run
        state.default_app = None


class CustomAppMixin(object):

    def setUp(self):
        super(CustomAppMixin, self).setUp()
        loop = self.loop = Loop()
        self.app = ThriftWorker(loop=loop)


class StartStopLoopMixin(CustomAppMixin):

    def setUp(self):
        super(StartStopLoopMixin, self).setUp()
        container = self.app.loop_container
        container.start()
        self.addCleanup(container.stop)

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

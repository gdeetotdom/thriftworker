from __future__ import absolute_import

import types
from time import time, sleep
from functools import wraps
from contextlib import contextmanager
from unittest import TestCase as BaseTestCase
from unittest.case import SkipTest

from pyuv import Loop, Async, Idle

from thriftworker import state
from thriftworker.app import ThriftWorker
from thriftworker.utils.env import detect_environment

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

    def setUp(self):
        # reset current state before each run
        state.default_app = None


class CustomAppMixin(object):

    def setUp(self):
        super(CustomAppMixin, self).setUp()
        loop = self.loop = Loop()
        self.app = ThriftWorker(loop=loop)

    def wait_for_predicate(self, func, timeout=TIMEOUT):
        tic = time()
        while func() and tic + timeout > time():
            sleep(0.1)


class StartStopLoopMixin(CustomAppMixin):

    def setUp(self):
        super(StartStopLoopMixin, self).setUp()
        hub = self.app.hub
        hub.start()
        self.addCleanup(hub.stop)

    def wakeup_loop(self):
        event = self.app.env.RealEvent()
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


def custom_env_needed(env):
    """Skip test if current environment differ from needed."""

    def inner_decorator(test_item):
        reason = 'Current environment differ from needed!'

        if env != detect_environment():
            if not isinstance(test_item, (type, types.ClassType)):
                @wraps(test_item)
                def skip_wrapper(*args, **kwargs):
                    raise SkipTest(reason)
                test_item = skip_wrapper

            test_item.__unittest_skip__ = True
            test_item.__unittest_skip_why__ = reason

        return test_item

    return inner_decorator

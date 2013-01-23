from __future__ import absolute_import

from greenlet import GreenletExit

from thriftworker.hub import sleep
from thriftworker.tests.utils import TestCase, CustomAppMixin, \
    start_stop_ctx, GreenTest


class ExpectedError(Exception):
    pass


class TestHub(CustomAppMixin, TestCase):

    def setUp(self):
        super(TestHub, self).setUp()
        self.hub = self.app.hub

    def context(self):
        hub = self.hub
        return start_stop_ctx(hub)

    def test_start_stop(self):
        hub = self.hub
        with self.context():
            self.assertIsNotNone(hub._guard)
            self.assertTrue(hub._guard.active)
        self.assertTrue(hub._started.is_set())
        self.assertTrue(hub._stopped.is_set())

    def test_wakeup(self):
        hub = self.hub
        with self.context():
            hub.wakeup()


class TestGreenlet(GreenTest):

    def test_simple_exit(self):
        link_test = []

        def waiting_func(seconds, value):
            sleep(seconds)
            print value
            return value

        g = self.hub.Greenlet(waiting_func, 0.01, value=5)
        g.rawlink(lambda x: link_test.append(x))
        assert not g, bool(g)
        assert not g.dead
        assert not g.ready()
        assert not g.successful()
        assert g.value is None
        assert g.exception is None

        g.start()
        assert g  # changed
        assert not g.dead
        assert not g.ready()
        assert not g.successful()
        assert g.value is None
        assert g.exception is None

        sleep(0.001)
        assert g
        assert not g.dead
        assert not g.ready()
        assert not g.successful()
        assert g.value is None
        assert g.exception is None
        assert not link_test

        sleep(0.02)
        assert not g
        assert g.dead
        assert g.ready()
        assert g.successful()
        assert g.value == 5
        assert g.exception is None  # not changed
        assert link_test == [g]  # changed

    def test_error_exit(self):
        link_test = []

        def func(delay, return_value=4):
            sleep(delay)
            error = ExpectedError('test_error_exit')
            error.myattr = return_value
            raise error

        g = self.hub.Greenlet(func, 0.001, return_value=5)
        g.rawlink(lambda x: link_test.append(x))
        g.start()
        sleep(0.1)
        assert not g
        assert g.dead
        assert g.ready()
        assert not g.successful()
        assert g.value is None  # not changed
        assert g.exception.myattr == 5
        assert link_test == [g], link_test

    def _assertKilled(self, g):
        assert not g
        assert g.dead
        assert g.ready()
        assert g.successful(), (repr(g), g.value, g.exception)
        assert isinstance(g.value, GreenletExit), (repr(g), g.value, g.exception)
        assert g.exception is None

    def assertKilled(self, g):
        self._assertKilled(g)
        sleep(0.01)
        self._assertKilled(g)

    def _test_kill(self, g, block):
        g.kill(block=block)
        if not block:
            sleep(0.01)
        self.assertKilled(g)
        # kill second time must not hurt
        g.kill(block=block)
        self.assertKilled(g)

    def _test_kill_not_started(self, block):
        link_test = []
        result = []
        g = self.hub.Greenlet(lambda: result.append(1))
        g.rawlink(lambda x: link_test.append(x))
        self._test_kill(g, block=block)
        assert not result
        assert link_test == [g]

    def test_kill_not_started_block(self):
        self._test_kill_not_started(block=True)

    def test_kill_not_started_noblock(self):
        self._test_kill_not_started(block=False)

    def _test_kill_just_started(self, block):
        result = []
        link_test = []
        g = self.hub.Greenlet(lambda: result.append(1))
        g.rawlink(lambda x: link_test.append(x))
        g.start()
        self._test_kill(g, block=block)
        assert not result, result
        assert link_test == [g]

    def test_kill_just_started_block(self):
        self._test_kill_just_started(block=True)

    def test_kill_just_started_noblock(self):
        self._test_kill_just_started(block=False)

    def _test_kill_running(self, block):
        link_test = []
        g = self.hub.spawn(sleep, 10)
        g.rawlink(lambda x: link_test.append(x))
        self._test_kill(g, block=block)
        sleep(0.01)
        assert link_test == [g]

    def test_kill_running_block(self):
        self._test_kill_running(block=True)

    def test_kill_running_noblock(self):
        self._test_kill_running(block=False)

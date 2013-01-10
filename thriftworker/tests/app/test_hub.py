from __future__ import absolute_import

from thriftworker.tests.utils import TestCase, CustomAppMixin, start_stop_ctx


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

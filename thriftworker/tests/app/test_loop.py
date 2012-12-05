from __future__ import absolute_import

from thriftworker.tests.utils import TestCase, CustomAppMixin, start_stop_ctx


class TestLoopContainer(CustomAppMixin, TestCase):

    def setUp(self):
        super(TestLoopContainer, self).setUp()
        self.container = self.app.loop_container

    def context(self):
        container = self.container
        return start_stop_ctx(container)

    def test_start_stop(self):
        container = self.container
        with self.context():
            self.assertIsNotNone(container._guard)
            self.assertTrue(container._guard.active)
        self.assertTrue(container._started.is_set())
        self.assertTrue(container._stopped.is_set())

    def test_wakeup(self):
        container = self.container
        with self.context():
            container.wakeup()

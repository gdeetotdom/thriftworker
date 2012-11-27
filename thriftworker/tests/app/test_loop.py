from __future__ import absolute_import

from contextlib import contextmanager

from pyuv import Loop

from thriftworker.tests.utils import TestCase
from thriftworker.app import ThriftWorker


class TestLoopContainer(TestCase):

    def setUp(self):
        super(TestLoopContainer, self).setUp()
        loop = self.loop = Loop()
        app = self.app = ThriftWorker(loop=loop)
        self.container = app.loop_container

    @contextmanager
    def context(self):
        container = self.container
        container.start()
        try:
            yield container
        finally:
            container.stop()

    def test_start_stop(self):
        loop = self.loop
        container = self.container
        with self.context():
            self.assertIsNotNone(container._guard)
            self.assertTrue(container._guard.active)
            self.assertEqual(1, loop.active_handles)
        self.assertTrue(container._started.is_set())
        self.assertTrue(container._stopped.is_set())
        self.assertEqual(0, loop.active_handles)

    def test_wakeup(self):
        container = self.container
        with self.context():
            container.wakeup()

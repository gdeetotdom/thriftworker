from __future__ import absolute_import

from thriftworker.tests.utils import TestCase
from thriftworker.state import get_current_app, set_current_app, current_app


class TestState(TestCase):

    def test_no_app_created(self):
        with self.assertRaises(RuntimeError):
            get_current_app()

    def test_change_app(self):
        some_app = object()
        set_current_app(some_app)
        self.assertIs(some_app, get_current_app())

    def test_current_app(self):
        some_app = object()
        set_current_app(some_app)
        self.assertIs(some_app, current_app._get_current_object())

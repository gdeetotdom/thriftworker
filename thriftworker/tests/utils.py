from __future__ import absolute_import

from unittest import TestCase as BaseTestCase

from thriftworker import state


class TestCase(BaseTestCase):

    def setUp(self):
        # reset current state before each run
        state.default_app = None

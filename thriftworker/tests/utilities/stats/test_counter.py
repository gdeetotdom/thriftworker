from __future__ import absolute_import

from thriftworker.tests.utils import TestCase
from thriftworker.utils.stats import Counter


class TestCounter(TestCase):

    def setUp(self):
        super(TestCounter, self).setUp()
        self.counter = Counter()

    def test_add(self):
        self.counter.add()
        self.assertEqual(1, int(self.counter))
        self.counter.add()
        self.assertEqual(2, int(self.counter))

    def test_add_sample(self):
        self.counter.add(5)
        self.assertEqual(5, int(self.counter))

    def test_iadd(self):
        self.counter += 1
        self.assertEqual(1, int(self.counter))

    def test_count(self):
        self.counter.add()
        self.counter.add()
        self.assertEqual(2, len(self.counter))

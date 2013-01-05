from __future__ import absolute_import

from thriftworker.tests.utils import TestCase
from thriftworker.utils.atomics import AtomicInteger


class TestAtomicInteger(TestCase):

    def setUp(self):
        self.integer = AtomicInteger()

    def test_cmp(self):
        self.assertEqual(0, self.integer)
        self.assertEqual(self.integer, self.integer.get())

    def test_get(self):
        self.assertEqual(0, self.integer.get())
        self.assertEqual(0, int(self.integer))

    def test_incr(self):
        self.assertEqual(1, self.integer.incr())
        self.assertEqual(1, self.integer.get())

    def test_decr(self):
        self.assertEqual(-1, self.integer.decr())
        self.assertEqual(-1, self.integer.get())

    def test_set(self):
        self.integer.set(5)
        self.assertEqual(5, self.integer.get())
        with self.assertRaises(TypeError):
            self.integer.set(None)

    def test_add(self):
        self.integer.add(5)
        self.assertEqual(5, self.integer.get())
        self.integer += 5
        self.assertEqual(10, self.integer.get())

    def test_sub(self):
        self.integer.sub(5)
        self.assertEqual(-5, self.integer.get())
        self.integer -= 5
        self.assertEqual(-10, self.integer.get())

    def test_repr(self):
        repr(self.integer)

    def test_props(self):
        self.assertEqual(0, self.integer.value)
        self.integer.value = 3
        self.assertEqual(3, self.integer.value)

from __future__ import absolute_import

from thriftworker.tests.utils import TestCase
from thriftworker.utils.other import get_port_from_range, \
    get_addresses_from_pool, rgetattr


def skip(g, num=1):
    for _ in xrange(num):
        g.next()


class TestOther(TestCase):

    def test_rgetattr(self):

        class Entity(object):
            pass

        root = Entity()
        branch1 = root.branch1 = Entity()
        leaf1 = root.branch1.leaf1 = Entity()

        self.assertIs(branch1, rgetattr(root, 'branch1'))
        self.assertIs(leaf1, rgetattr(root, 'branch1.leaf1'))

    def test_port_from_range(self):
        name = 'SomeService'
        new_port = get_port_from_range(name, 10000, 20000)
        self.assertEqual(13618, new_port)

        name = 'SomeService' * 100
        new_port = get_port_from_range(name, 10000, 11000)
        self.assertEqual(10834, new_port)

    def test_addresses_from_pool_digit(self):
        name = 'SomeService'
        address = 'localhost', 10500

        self.assertEqual((address,),
            tuple(get_addresses_from_pool(name, address)))
        self.assertEqual((address,),
            tuple(get_addresses_from_pool(name, (address[0], str(address[1])))))
        self.assertEqual((address,),
            tuple(get_addresses_from_pool(name, address, (11000, 12000))))

    def test_addresses_from_pool_auto(self):
        name = 'SomeService'
        address = 'localhost', None

        self.assertEqual(((address[0], 0),),
            tuple(get_addresses_from_pool(name, address)))

        g = get_addresses_from_pool(name, address, (11000, 12000))
        self.assertEqual((address[0], 11538), g.next())
        self.assertEqual((address[0], 11539), g.next())
        skip(g, 99)
        self.assertEqual((address[0], 11639), g.next())
        skip(g, 12000 - 11639 - 1)
        self.assertEqual((address[0], 11000), g.next())
        skip(g, 11538 - 11000 - 2)
        self.assertEqual((address[0], 11537), g.next())
        self.assertEqual([], list(g))

    def test_addresses_from_pool_wrong(self):
        name = 'SomeService'
        with self.assertRaises(ValueError):
            list(get_addresses_from_pool(name, ('localhost', 'unknown')))
        with self.assertRaises(ValueError):
            list(get_addresses_from_pool(name, ('localhost', '')))
        with self.assertRaises(ValueError):
            list(get_addresses_from_pool(name, ('localhost', object())))

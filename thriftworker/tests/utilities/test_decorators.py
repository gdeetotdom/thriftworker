from __future__ import absolute_import

from thriftworker.tests.utils import TestCase
from thriftworker.utils.decorators import cached_property


class TestOther(TestCase):

    def test_cached_property(self):

        def fun(obj):
            return

        x = cached_property(fun)
        self.assertIs(x.__get__(None), x)
        self.assertIs(x.__set__(None, None), x)
        self.assertIs(x.__delete__(None), x)

    def test_cached_property_access(self):
        actions = []

        class Entity(object):

            @cached_property
            def prop(self):
                actions.append('new')
                return 1

            @prop.setter
            def prop(self, value):
                actions.append('set')
                return int(value)

            @prop.deleter
            def prop(self, value):
                actions.append('del')

        entity = Entity()
        self.assertEqual(1, entity.prop)
        self.assertEqual(['new'], actions)
        self.assertIn('prop', vars(entity))

        entity.prop = '2'
        self.assertEqual(2, entity.prop)
        self.assertEqual(['new', 'set'], actions)

        del entity.prop
        self.assertEqual(['new', 'set', 'del'], actions)
        self.assertNotIn('prop', vars(entity))
        del entity.prop
        self.assertEqual(1, entity.prop)
        self.assertEqual(['new', 'set', 'del', 'new'], actions)

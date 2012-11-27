from __future__ import absolute_import

from thriftworker.tests.utils import TestCase
from thriftworker.utils.finalize import Finalize


class TestOther(TestCase):

    def test_out_of_scope(self):
        actions = []
        finalizers = []

        class Entity(object):

            def __init__(self):
                finalizers.append(Finalize(self,
                    lambda: actions.append('finalized')))

        create_entity = lambda: Entity()
        create_entity()
        self.assertEqual(['finalized'], actions)
        self.assertFalse(finalizers[0].still_active())

    def test_reset(self):
        actions = []
        finalizers = []

        class Entity(object):

            def __init__(self):
                finalizers.append(Finalize(self,
                    lambda: actions.append('finalized')))

        def create_entity():
            entity = Entity()
            finalizers[0].cancel()
            return entity
        create_entity()
        self.assertEqual([], actions)

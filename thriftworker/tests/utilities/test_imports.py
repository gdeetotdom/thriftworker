from __future__ import absolute_import

from mock import Mock

from thriftworker.tests.utils import TestCase
from thriftworker.utils.imports import symbol_by_name


class TestSymbolByName(TestCase):

    def test_instance_returns_instance(self):
        instance = object()
        self.assertIs(symbol_by_name(instance), instance)

    def test_returns_default(self):
        default = object()
        self.assertIs(symbol_by_name('xyz.ryx.qedoa.weq:foz',
                                     default=default), default)

    def test_no_default(self):
        with self.assertRaises(ImportError):
            symbol_by_name('xyz.ryx.qedoa.weq:foz')

    def test_imp_reraises_ValueError(self):
        imp = Mock()
        imp.side_effect = ValueError()
        with self.assertRaises(ValueError):
            symbol_by_name('thriftworker:ThriftWorker', imp=imp)

    def test_package(self):
        from thriftworker.hub import Hub
        self.assertIs(symbol_by_name('.hub:Hub',
                    package='thriftworker'), Hub)
        self.assertTrue(symbol_by_name(':ThriftWorker',
                                       package='thriftworker'))

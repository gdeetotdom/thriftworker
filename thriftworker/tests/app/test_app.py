from __future__ import absolute_import

from pyuv import Loop
from thrift.protocol import TBinaryProtocol

from thriftworker.tests.utils import TestCase
from thriftworker.app import ThriftWorker
from thriftworker.state import get_current_app


class TestApp(TestCase):

    def test_current_app(self):
        some_app = ThriftWorker()
        self.assertIs(some_app, get_current_app())

    def test_default_app(self):
        some_app = ThriftWorker()
        global_app = ThriftWorker.instance()
        self.assertIs(some_app, global_app)

    def test_negative_pool_size(self):
        with self.assertRaises(ValueError):
            ThriftWorker(pool_size=-1)

    def test_wrong_port_range(self):
        with self.assertRaises(ValueError):
            ThriftWorker(port_range=())
            ThriftWorker(port_range=(1, 'test'))

    def test_custom_loop(self):
        custom_loop = Loop()
        app = ThriftWorker(loop=custom_loop)
        self.assertIs(custom_loop, app.loop)

    def test_custom_proto_factory(self):
        factory = TBinaryProtocol.TBinaryProtocolFactory()
        app = ThriftWorker(protocol_factory=factory)
        self.assertIs(factory, app.protocol_factory)

    def test_custom_port_range(self):
        app = ThriftWorker(port_range=(1, 10))
        self.assertEqual((1, 10), app.port_range)

        app = ThriftWorker(port_range=('1', '10'))
        self.assertEqual((1, 10), app.port_range)

    def test_custom_pool_size(self):
        app = ThriftWorker(pool_size=5)
        self.assertEqual(5, app.pool_size)

from __future__ import absolute_import

from cStringIO import StringIO

from mock import Mock

from thriftworker.tests.utils import TestCase, CustomAppMixin


class TestServices(CustomAppMixin, TestCase):

    def setUp(self):
        super(TestServices, self).setUp()
        self.services = self.app.services
        self.service_name = 'SomeService'
        self.processor = Mock()

    def test_register(self):
        self.services.register(self.service_name, self.processor)
        self.assertIn(self.service_name, self.services)
        service = self.services[self.service_name]
        self.assertIs(self.processor, service.processor)

    def test_processor(self):
        self.services.register(self.service_name, self.processor)
        process_mock = self.processor.process = Mock(return_value=None)
        process = self.services.create_processor(self.service_name)
        self.assertEqual((None, ''), process(StringIO(b'xxxx')))
        self.assertTrue(process_mock.called)
        self.assertEqual(1, process_mock.call_count)

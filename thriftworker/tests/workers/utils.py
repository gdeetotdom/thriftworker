from __future__ import absolute_import

from cStringIO import StringIO

from mock import Mock

from thriftworker.tests.utils import StartStopLoopMixin, start_stop_ctx


class WorkerMixin(StartStopLoopMixin):

    Worker = None

    def setUp(self):
        super(WorkerMixin, self).setUp()
        Worker = self.app.subclass_with_self(type(self).Worker)
        self.Worker = self.app.Worker = Worker
        service_name = self.service_name = 'SomeService'
        processor = self.processor = Mock()
        self.app.services.register(service_name, processor)

    def check_request(self, worker):
        connection, data, request_id = Mock(), StringIO(''), 1
        with start_stop_ctx(worker):
            producer = worker.create_producer(self.service_name)
            producer(connection, data, request_id)
            self.wait_for_predicate(lambda: not connection.ready.called)
            self.assertEqual(1, self.processor.process.call_count)
            self.assertEqual(1, connection.ready.call_count)
            self.assertEqual((True, '', 1), connection.ready.call_args[0])

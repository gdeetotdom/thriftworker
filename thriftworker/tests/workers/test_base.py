from __future__ import absolute_import

from mock import Mock

from thriftworker.workers.base import BaseWorker
from thriftworker.tests.utils import TestCase, start_stop_ctx

from .utils import WorkerMixin


class Worker(BaseWorker):

    def __init__(self, consumer):
        self.consumer = consumer
        super(Worker, self).__init__()

    def create_consumer(self, processor):
        return self.consumer


class TestBaseWorker(WorkerMixin, TestCase):

    Worker = Worker

    def start_worker(self):
        consumer = Mock()
        worker = self.Worker(consumer)
        return start_stop_ctx(worker)

    def test_producer(self):
        connection, data, request_id = object(), object(), object()
        with self.start_worker() as worker:
            producer = worker.create_producer(self.service_name)
            producer(connection, data, request_id)
            consumer = worker.consumer
            self.assertEqual(1, consumer.call_count)
            args, kwargs = consumer.call_args
            request = args[0]
            self.assertIsInstance(request, self.Worker.Request)
            self.assertIs(connection, request.connection)
            self.assertIs(data, request.data)
            self.assertIs(request_id, request.request_id)

    def test_callback(self):
        connection, data, request_id, result = \
            Mock(), object(), object(), object()
        connection.is_waiting.return_value = True
        with self.start_worker() as worker:
            request = self.Worker.Request(connection, data, request_id)
            callback = worker.create_callback(request)
            callback(result)
            self.assertEqual(1, connection.ready.call_count)
            args, kwargs = connection.ready.call_args
            self.assertTrue(args[0])
            self.assertIs(result, args[1])
            self.assertIs(request_id, args[2])

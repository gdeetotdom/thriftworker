from __future__ import absolute_import

from mock import Mock

from thriftworker.workers.base import BaseWorker
from thriftworker.tests.utils import TestCase, start_stop_ctx

from .utils import WorkerMixin


class Worker(BaseWorker):

    def __init__(self, consumer):
        self.consumer = consumer
        super(Worker, self).__init__()

    def create_consumer(self):
        return self.consumer


class TestBaseWorker(WorkerMixin, TestCase):

    Worker = Worker

    def create_worker(self):
        consumer = Mock()
        worker = self.Worker(consumer)
        return worker

    def test_producer(self):
        connection, data, request_id = \
            object(), object(), object()
        with start_stop_ctx(self.create_worker()) as worker:
            producer = worker.create_producer(self.service_name)
            producer(connection, data, request_id)
            consumer = worker.consumer
            self.assertEqual(1, consumer.call_count)
            args, kwargs = consumer.call_args
            self.assertEqual(2, len(args))
            self.assertTrue(callable(args[0]))
            self.assertTrue(callable(args[1]))

    def test_callback(self):
        connection, data, request_id, result = \
            Mock(), object(), object(), (None, object())
        connection.is_waiting.return_value = True
        with start_stop_ctx(self.create_worker()) as worker:
            request = self.Worker.Request(
                self.loop, connection, data, request_id, self.service_name)
            request.execute(Mock(return_value=result))
            callback = worker.create_callback()
            callback(request, result)
            self.assertEqual(1, connection.ready.call_count)
            args, kwargs = connection.ready.call_args
            self.assertTrue(args[0])
            self.assertIs(result[1], args[1])
            self.assertIs(request_id, args[2])

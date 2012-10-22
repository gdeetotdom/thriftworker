"""Ventilator that push new request to queue."""
from collections import deque

from pyuv import ThreadPool

from .mixin import LoopMixin
from .utils import cached_property

__all__ = ['Ventilator']


class WorkerPool(object):

    def __init__(self):
        self.pool = deque()

    def release(self, worker):
        self.pool.append(worker)

    def acquire(self):
        return self.pool.popleft()


class Ventilator(LoopMixin):
    """Push new request for processing."""

    app = None

    def __init__(self):
        self.workers = set()

    @cached_property
    def worker_pool(self):
        return WorkerPool()

    @cached_property
    def thread_pool(self):
        return ThreadPool(self.loop)

    def register(self, worker):
        self.workers.add(worker)
        self.worker_pool.release(worker)
        self.thread_pool.set_parallel_threads(len(self.workers))

    def remove(self, worker):
        self.workers.remove(worker)

    def create_producer(self, service):
        """Produce new request."""
        worker_pool = self.worker_pool
        thread_pool = self.thread_pool

        def producer(connection, request):

            def cb_work():
                worker = worker_pool.acquire()
                success, response = worker.process(service, request)
                return worker, success, response

            def cb_after_work(result, exception):
                worker, success, response = result
                worker_pool.release(worker)
                connection.ready(success, response)

            thread_pool.queue_work(cb_work, cb_after_work)

        return producer

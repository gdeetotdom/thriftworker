"""Ventilator that push new request to queue."""
import logging

from pyuv import ThreadPool

from .mixin import LoopMixin
from .pool import ResourcePool
from .utils import cached_property

__all__ = ['Ventilator']

logger = logging.getLogger(__name__)


class Ventilator(LoopMixin):
    """Push new request for processing."""

    app = None

    def __init__(self):
        self.workers = set()

    @cached_property
    def worker_pool(self):
        return ResourcePool()

    @cached_property
    def thread_pool(self):
        return ThreadPool(self.loop)

    def register(self, worker):
        self.workers.add(worker)
        self.worker_pool.add(worker)
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
                try:
                    success, response = worker.process(service, request)
                finally:
                    worker.release()
                return success, response

            def cb_after_work(result, exception):
                if exception is None:
                    success, response = result
                else:
                    logger.error(exception)
                    success, response = False, ''
                connection.ready(success, response)

            thread_pool.queue_work(cb_work, cb_after_work)

        return producer

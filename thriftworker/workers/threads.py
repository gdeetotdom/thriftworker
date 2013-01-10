from __future__ import absolute_import

import sys
import logging
from threading import Thread, Event
from Queue import Queue, Empty

from thriftworker.utils.decorators import cached_property
from thriftworker.workers.base import BaseWorker

logger = logging.getLogger(__name__)


class Worker(Thread):
    """Simple threaded worker."""

    def __init__(self, tasks):
        super(Worker, self).__init__()
        self.daemon = True
        self.tasks = tasks
        self._is_shutdown = Event()
        self._is_stopped = Event()

    def body(self):
        """Consume messages from queue and execute them until empty message
        sent.

        """
        tasks = self.tasks
        shutdown = self._is_shutdown.set
        while True:
            try:
                message = tasks.get()
            except Empty:
                break
            if message is None:
                shutdown()
                break
            result, exception = None, None
            task, callback = message
            try:
                result = task()
            except:
                exception = sys.exc_info()
            callback(result, exception)

    def run(self):
        shutdown_set = self._is_shutdown.is_set
        body = self.body
        try:
            while not shutdown_set():
                body()
        finally:
            self._is_stopped.set()

    def wait(self):
        self._is_stopped.wait()


class Pool(object):
    """Orchestrate workers. Start and stop them, provide new tasks."""

    Worker = Worker

    def __init__(self, size=None):
        self.size = size or 1
        self._tasks = Queue()

    @cached_property
    def _workers(self):
        return [self.Worker(self._tasks) for i in xrange(self.size)]

    def put(self, task):
        self._tasks.put_nowait(task)

    def start(self):
        for worker in self._workers:
            worker.start()

    def stop(self):
        for _ in self._workers:
            self._tasks.put(None)
        for worker in self._workers:
            worker.wait()


class ThreadsWorker(BaseWorker):
    """Process all request in thread-pool."""

    @cached_property
    def _pool(self):
        return Pool(self.app.pool_size)

    def create_consumer(self):
        pool = self._pool
        execute = self.app.hub.callback

        def inner_consumer(task, callback):
            pool.put((task, lambda *args: execute(callback, *args)))

        return inner_consumer

    def start(self):
        self._pool.start()

    def stop(self):
        self._pool.stop()

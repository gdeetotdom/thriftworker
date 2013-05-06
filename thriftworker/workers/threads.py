from __future__ import absolute_import

import sys
import logging
from collections import namedtuple
from threading import Thread, Event

from ..utils.decorators import cached_property

from .base import BaseWorker
from .queue import Queue

logger = logging.getLogger(__name__)


class Worker(Thread):
    """Simple threaded worker."""

    def __init__(self, app, queue, shutdown_timeout=None):
        super(Worker, self).__init__()
        self.app = app
        self.daemon = True
        self.queue = queue
        self._is_shutdown = Event()
        self._is_stopped = Event()
        self.shutdown_timeout = shutdown_timeout or 5.0

    def body(self):
        """Consume messages from queue and execute them until empty message
        sent.

        """
        get = self.queue.get
        shutdown = self._is_shutdown.set
        delay = self.app.hub.callback

        while True:
            message = get()
            if message is None:
                shutdown()
                break
            result = None
            exception = None
            try:
                result = message.task()
            except Exception:
                exception = sys.exc_info()
            delay(message.callback, result, exception)

    def run(self):
        shutdown_set = self._is_shutdown.is_set
        body = self.body
        try:
            while not shutdown_set():
                body()
        finally:
            self._is_stopped.set()

    def wait(self):
        self._is_stopped.wait(self.shutdown_timeout)


class Pool(object):
    """Orchestrate workers. Start and stop them, provide new tasks."""

    Worker = Worker

    def __init__(self, app, size=None):
        self.app = app
        self.size = size or 1
        self.queue = Queue()

    @cached_property
    def _workers(self):
        return [self.Worker(self.app, self.queue) for i in xrange(self.size)]

    def put(self, task):
        self.queue.put_nowait(task)

    def start(self):
        for worker in self._workers:
            worker.start()

    def stop(self):
        for _ in self._workers:
            self.queue.put(None)
        for worker in self._workers:
            worker.wait()


class ThreadsWorker(BaseWorker):
    """Process all request in thread-pool."""

    Message = namedtuple('Message', ('task', 'callback'))

    @cached_property
    def _pool(self):
        return Pool(self.app, size=self.app.pool_size)

    def create_consumer(self):
        pool = self._pool
        Message = self.Message

        def inner_consumer(task, callback):
            pool.put(Message(task, callback))

        return inner_consumer

    def start(self):
        self._pool.start()

    def stop(self):
        self._pool.stop()

from __future__ import absolute_import

import logging
from threading import Thread, Event
from collections import deque
from Queue import Queue, Empty
from functools import partial

from pyuv import Prepare, Async

from thriftworker.utils.decorators import cached_property
from thriftworker.workers.base import BaseWorker

logger = logging.getLogger(__name__)


class Worker(Thread):

    def __init__(self, tasks, callback, wakeup):
        super(Worker, self).__init__()
        self.daemon = True
        self.tasks = tasks
        self.callback = callback
        self.wakeup = wakeup
        self._is_shutdown = Event()
        self._is_stopped = Event()

    def body(self):
        cb = self.callback
        tasks = self.tasks
        wakeup = self.wakeup
        shutdown = self._is_shutdown.set
        while True:
            try:
                task = tasks.get()
            except Empty:
                break
            if task is None:
                shutdown()
                break
            exception = None
            processor, request, callback = task
            try:
                result = processor(request.data)
            except Exception as exc:
                exception = exc
            cb(partial(callback, result, exception))
            wakeup()

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

    Worker = Worker

    def __init__(self, wakeup, callback, size=None):
        tasks = self._tasks = Queue()
        self._workers = [self.Worker(tasks, callback, wakeup)
                         for _ in xrange(size or 1)]

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
    """Process all request in threadpool."""

    def __init__(self):
        self._outgoing = deque()
        super(ThreadsWorker, self).__init__()

    @cached_property
    def _prepare_handle(self):
        return Prepare(self.loop)

    @cached_property
    def _async_handle(self):
        return Async(self.loop, lambda handle: None)

    @cached_property
    def _pool(self):
        async = self._async_handle
        outgoing = self._outgoing
        wakeup = lambda: async.send()
        callback = lambda fn: outgoing.append(fn)
        return Pool(wakeup=wakeup, callback=callback,
                    size=self.app.pool_size)

    def _before_iteration(self, handle):
        outgoing = self._outgoing
        while True:
            try:
                callback = outgoing.popleft()
            except IndexError:
                break
            else:
                callback()

    def create_consumer(self, processor):
        create_callback = self._create_callback
        pool = self._pool

        def inner_consumer(request):
            pool.put((processor, request, create_callback(request)))

        return inner_consumer

    def start(self):
        self._prepare_handle.start(self._before_iteration)
        self._pool.start()

    def stop(self):
        self._pool.stop()
        self._async_handle.close()
        self._prepare_handle.close()

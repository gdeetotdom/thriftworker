from __future__ import absolute_import

import sys
import logging

from thriftworker.workers.base import BaseWorker

logger = logging.getLogger(__name__)


class Promise(object):
    """Used to enqueue task execution in thread pool."""

    __slots__ = ('func', 'callback', 'result', 'exception')

    def __init__(self, func, callback):
        self.func = func
        self.callback = callback
        self.result = None
        self.exception = None

    def __call__(self):
        try:
            self.result = self.func()
        except:
            self.exception = sys.exc_info()

    def cb(self, *args):
        self.callback(self.result, self.exception)


class SyncWorker(BaseWorker):
    """Process all request in separate thread."""

    def create_consumer(self):
        loop = self.loop

        def inner_consumer(task, callback):
            """Nested function that process incoming request."""
            promise = Promise(task, callback)
            loop.queue_work(promise, promise.cb)

        return inner_consumer


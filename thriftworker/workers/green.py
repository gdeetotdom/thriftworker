from __future__ import absolute_import

import sys
import logging
from collections import deque

from gevent.hub import get_hub
from gevent.pool import Pool

from thriftworker.workers.base import BaseWorker
from thriftworker.utils.decorators import cached_property

logger = logging.getLogger(__name__)


class GeventWorker(BaseWorker):

    def __init__(self, pool_size=None):
        self._incoming = deque()
        self._outgoing = deque()
        super(GeventWorker, self).__init__(pool_size)

    @cached_property
    def _pool(self):
        return Pool(size=self.pool_size)

    @cached_property
    def _worker_prepare_handle(self):
        return get_hub().loop.prepare()

    @cached_property
    def _worker_async_handle(self):
        return get_hub().loop.async()

    def create_consumer(self):
        execute = self.app.hub.callback
        incoming = self._incoming
        async = self._worker_async_handle

        def inner_consumer(task, callback):
            incoming.append((task, lambda *args: execute(callback, *args)))
            async.send()

        return inner_consumer

    def _process_request(self, task, callback):
        result, exception = None, None
        try:
            result = task()
        except:
            exception = sys.exc_info()
        callback(result, exception)

    def _before_worker_iteration(self):
        pool = self._pool
        process_request = self._process_request
        incoming = self._incoming
        while True:
            try:
                args = incoming.popleft()
            except IndexError:
                break
            else:
                pool.apply_async(process_request, args)

    def start(self):
        """Start worker and all gevent handles."""
        self._worker_prepare_handle.start(self._before_worker_iteration)
        self._worker_async_handle.start(lambda: None)

    def stop(self):
        """Stop pool and all started handles."""
        self._pool.join()
        self._worker_async_handle.stop()
        self._worker_prepare_handle.stop()

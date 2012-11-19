from __future__ import absolute_import

import logging
from functools import partial
from collections import deque

from pyuv import Prepare, Async
from gevent.hub import get_hub
from gevent.pool import Pool

from thriftworker.workers.base import BaseWorker
from thriftworker.utils.decorators import cached_property

logger = logging.getLogger(__name__)


class GeventWorker(BaseWorker):

    def __init__(self):
        self._incoming = deque()
        self._outgoing = deque()
        super(GeventWorker, self).__init__()

    @cached_property
    def _pool(self):
        return Pool(size=self.app.pool_size)

    @cached_property
    def _acceptor_prepare_handle(self):
        return Prepare(self.loop)

    @cached_property
    def _acceptor_async_handle(self):
        return Async(self.loop, lambda handle: None)

    @cached_property
    def _worker_prepare_handle(self):
        return get_hub().loop.prepare()

    @cached_property
    def _worker_async_handle(self):
        return get_hub().loop.async()

    def create_consumer(self, processor):
        incoming = self._incoming
        async = self._worker_async_handle
        create_callback = self._create_callback

        def inner_consumer(request):
            incoming.append((processor, request, create_callback(request)))
            async.send()

        return inner_consumer

    def _process_request(self, processor, request, callback):
        result, exception = None, None
        try:
            result = processor(request.data)
        except Exception as exc:
            exception = exc
        self._outgoing.append(partial(callback, result, exception))
        self._acceptor_async_handle.send()

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

    def _before_acceptor_iteration(self, handle):
        outgoing = self._outgoing
        while True:
            try:
                callback = outgoing.popleft()
            except IndexError:
                break
            else:
                callback()

    def start(self):
        self._acceptor_prepare_handle.start(self._before_acceptor_iteration)
        self._worker_prepare_handle.start(self._before_worker_iteration)
        self._worker_async_handle.start(lambda: None)

    def stop(self):
        self._pool.join()
        self._worker_async_handle.stop()
        self._worker_prepare_handle.stop()
        self._acceptor_async_handle.close()
        self._acceptor_prepare_handle.close()

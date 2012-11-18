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

    @property
    def _hub(self):
        return get_hub()

    @cached_property
    def _pool(self):
        return Pool()

    @cached_property
    def _pyuv_prepare_handle(self):
        return Prepare(self.loop)

    @cached_property
    def _pyuv_async_handle(self):
        return Async(self.loop, lambda handle: None)

    @cached_property
    def _gevent_prepare_handle(self):
        return self._hub.loop.prepare()

    @cached_property
    def _gevent_async_handle(self):
        return self._hub.loop.async()

    def create_consumer(self, processor):
        incoming = self._incoming
        async = self._gevent_async_handle
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
        self._pyuv_async_handle.send()

    def _before_gevent_iteration(self):
        pool = self._pool
        while True:
            try:
                args = self._incoming.popleft()
            except IndexError:
                break
            else:
                pool.apply_async(self._process_request, args=args)

    def _before_pyuv_iteration(self, handle):
        while True:
            try:
                callback = self._outgoing.popleft()
            except IndexError:
                break
            else:
                callback()

    def start(self):
        self._pyuv_prepare_handle.start(self._before_pyuv_iteration)
        self._gevent_prepare_handle.start(self._before_gevent_iteration)
        self._gevent_async_handle.start(lambda: None)

    def stop(self):
        self._gevent_async_handle.stop()
        self._gevent_prepare_handle.stop()
        self._pyuv_async_handle.close()
        self._pyuv_prepare_handle.close()

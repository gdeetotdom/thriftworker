"""Base pool implementation."""
from __future__ import absolute_import

from functools import partial

from pyuv import ThreadPool

from .base import BasePool
from ..utils.decorators import cached_property


class SoloPool(BasePool):
    """Process all request in separate thread."""

    def start(self):
        pass

    def stop(self):
        pass

    @cached_property
    def _pool(self):
        return ThreadPool(self.app.loop)

    def queue_request(self, request, callback):
        work = partial(self._process_request, request.service, request.data)
        self._pool.queue_work(work, callback)

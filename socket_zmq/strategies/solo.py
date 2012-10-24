"""Base pool implementation."""
from __future__ import absolute_import

from pyuv import ThreadPool

from .base import BasePool
from ..utils.decorators import cached_property


class SoloPool(BasePool):
    """Process all request in separate thread."""

    @cached_property
    def _pool(self):
        return ThreadPool(self.app.loop)

    def queue_request(self, service, connection, request):

        def cb_work():
            return self.process(service, request)

        def cb_after_work(result, exception):
            self.request_done(service, connection, result, exception)

        self._pool.queue_work(cb_work, cb_after_work)

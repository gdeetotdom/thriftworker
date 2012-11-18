from __future__ import absolute_import

import logging

from pyuv import ThreadPool

from thriftworker.utils.decorators import cached_property
from thriftworker.workers.base import BaseWorker

logger = logging.getLogger(__name__)


class SyncWorker(BaseWorker):
    """Process all request in separate thread."""

    @cached_property
    def pool(self):
        return ThreadPool(self.loop)

    def create_consumer(self, processor):
        create_callback = self._create_callback
        pool = self.pool

        def inner_consumer(request):
            # Nested function that process incoming request.
            task = lambda: processor(request.data)
            # Put task to pool.
            pool.queue_work(task, create_callback(request))

        return inner_consumer

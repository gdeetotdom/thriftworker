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

    def create_consumer(self):
        pool = self.pool

        def inner_consumer(task, callback):
            """Nested function that process incoming request."""
            pool.queue_work(task, callback)

        return inner_consumer

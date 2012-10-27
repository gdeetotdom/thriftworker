"""Base pool implementation."""
from __future__ import absolute_import

import logging
from collections import namedtuple

from pyuv import ThreadPool

from .utils.decorators import cached_property

logger = logging.getLogger(__name__)


class Worker(object):
    """Process all request in separate thread."""

    app = None

    #: Store request in this tuple.
    Request = namedtuple('Request', 'connection data')

    @cached_property
    def pool(self):
        return ThreadPool(self.app.loop)

    def create_callback(self, request):
        """Create callback that should be called after request was done."""
        connection = request.connection

        def inner_callback(result, exception):
            if exception is None:
                success, response = True, result
            else:
                logger.error(exception)
                success, response = False, ''
            connection.ready(success, response)

        return inner_callback

    def create_consumer(self, processor):
        create_callback = self.create_callback
        pool = self.pool

        def inner_consumer(request):
            # Nested function that process incoming request.
            task = lambda: processor(request.data)
            # Put task to pool.
            pool.queue_work(task, create_callback(request))

        return inner_consumer

    def create_producer(self, service):
        """Create producer for connections."""
        Request = self.Request
        processor = self.app.services.create_processor(service)
        consume = self.create_consumer(processor)

        def inner_producer(connection, data):
            """Enqueue given request to thread pool."""
            consume(Request(connection=connection, data=data))

        return inner_producer

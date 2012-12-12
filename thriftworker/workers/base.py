from __future__ import absolute_import

import logging
from collections import namedtuple
from abc import ABCMeta, abstractmethod

from thriftworker.utils.mixin import LoopMixin
from thriftworker.utils.loop import in_loop

logger = logging.getLogger(__name__)


class BaseWorker(LoopMixin):

    __metaclass__ = ABCMeta

    #: Store request in this tuple.
    Request = namedtuple('Request', 'connection data request_id')

    def create_callback(self, request):
        """Create callback that should be called after request was done."""
        connection = request.connection
        request_id = request.request_id

        def inner_callback(result, exception=None):
            if exception is None:
                success, response = True, result
            else:
                logger.error(exception[1], exc_info=exception)
                success, response = False, ''
            if connection.is_waiting():
                connection.ready(success, response, request_id)

        return inner_callback

    @abstractmethod
    def create_consumer(self, processor):
        raise NotImplementedError()

    def create_producer(self, service):
        """Create producer for connections."""
        Request = self.Request
        processor = self.app.services.create_processor(service)
        consume = self.create_consumer(processor)

        def inner_producer(connection, data, request_id):
            """Enqueue given request to thread pool."""
            consume(Request(connection=connection,
                            data=data,
                            request_id=request_id))

        return inner_producer

    @in_loop
    def start(self):
        """Start worker. By default do nothing."""

    @in_loop
    def stop(self):
        """Stop worker. By default do nothing."""

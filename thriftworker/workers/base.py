from __future__ import absolute_import

import logging
from collections import namedtuple
from abc import ABCMeta, abstractmethod
from functools import partial

from six import with_metaclass

from thriftworker.utils.mixin import LoopMixin, StartStopMixin

logger = logging.getLogger(__name__)


class BaseWorker(StartStopMixin, with_metaclass(ABCMeta, LoopMixin)):

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
    def create_consumer(self):
        raise NotImplementedError()

    def create_task(self, processor):
        """Create new task for given processor."""

        def inner_task(request):
            """Process incoming request with given processor."""
            return processor(request.data)

        return inner_task

    def create_producer(self, service):
        """Create producer for connections."""
        Request = self.Request
        create_callback = self.create_callback
        processor = self.app.services.create_processor(service)
        task = self.create_task(processor)
        consume = self.create_consumer()

        def inner_producer(connection, data, request_id):
            """Enqueue given request to thread pool."""
            request = Request(connection=connection,
                              data=data,
                              request_id=request_id)
            curried_task = partial(task, request)
            callback = create_callback(request)
            consume(curried_task, callback)

        return inner_producer

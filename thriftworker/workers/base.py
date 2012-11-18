from __future__ import absolute_import

import logging
from collections import namedtuple
from abc import ABCMeta, abstractmethod

from thriftworker.utils.mixin import LoopMixin

logger = logging.getLogger(__name__)


class BaseWorker(LoopMixin):

    __metaclass__ = ABCMeta

    #: Store request in this tuple.
    Request = namedtuple('Request', 'connection data')

    def _create_callback(self, request):
        """Create callback that should be called after request was done."""
        connection = request.connection

        def inner_callback(result, exception=None):
            if exception is None:
                success, response = True, result
            else:
                logger.error(exception)
                success, response = False, ''
            connection.ready(success, response)

        return inner_callback

    @abstractmethod
    def create_consumer(self, processor):
        raise NotImplementedError()

    def create_producer(self, service):
        """Create producer for connections."""
        Request = self.Request
        processor = self.app.services.create_processor(service)
        consume = self.create_consumer(processor)

        def inner_producer(connection, data):
            """Enqueue given request to thread pool."""
            consume(Request(connection=connection, data=data))

        return inner_producer

    def start(self):
        pass

    def stop(self):
        pass

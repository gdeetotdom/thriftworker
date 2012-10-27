"""Base pool implementation."""
from __future__ import absolute_import

from collections import namedtuple
from functools import partial
from abc import ABCMeta, abstractmethod
import logging

logger = logging.getLogger(__name__)

__all__ = ['BasePool']


class BasePool(object):

    __metaclass__ = ABCMeta

    app = None

    Request = namedtuple('Request', 'service connection data')

    @abstractmethod
    def start(self):
        """Start processing pool."""
        raise NotImplementedError()

    @abstractmethod
    def stop(self):
        """Stop processing pool."""
        raise NotImplementedError()

    @abstractmethod
    def queue_request(self, request, callback):
        """Enqueue request to processing pool."""
        raise NotImplementedError()

    def _process_request(self, service, data):
        """Dispatch given request. Should be called from workers."""
        return self.app.processor(service, data)

    def _request_done(self, request, result, exception):
        """Request done, send response client"""
        if exception is None:
            success, response = True, result
        else:
            logger.error(exception)
            success, response = False, ''
        request.connection.ready(success, response)

    def create_producer(self, service):
        """Create producer for connections."""
        Request = self.Request
        request_done = self._request_done
        queue_request = self.queue_request

        def producer(connection, data):
            """Process given request."""
            request = Request(service=service,
                              connection=connection,
                              data=data)
            callback = partial(request_done, request)
            queue_request(request, callback)

        return producer

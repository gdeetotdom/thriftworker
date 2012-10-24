"""Base pool implementation."""
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
from functools import partial
import logging

logger = logging.getLogger(__name__)

__all__ = ['BasePool']


class BasePool(object):

    __metaclass__ = ABCMeta

    app = None

    @abstractmethod
    def queue_request(self, service, connection, request):
        raise NotImplementedError()

    def create_producer(self, service):
        """Create producer for connections."""
        return partial(self.queue_request, service)

    def process(self, service, request):
        return self.app.processor(service, request)

    def request_done(self, service, connection, result, exception):
        """Request done, send response client"""
        if exception is None:
            success, response = result
        else:
            logger.error(exception)
            success, response = False, ''
        connection.ready(success, response)

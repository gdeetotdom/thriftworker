from __future__ import absolute_import

import logging
from abc import ABCMeta, abstractmethod
from functools import partial

from six import with_metaclass

from thriftworker.utils.mixin import LoopMixin, StartStopMixin
from thriftworker.utils.atomics import ContextCounter
from thriftworker.utils.decorators import cached_property

logger = logging.getLogger(__name__)


class Request(object):

    __slots__ = ('connection', 'message_buffer', 'request_id', 'service',
                 'start_time')

    def __init__(self, connection, message_buffer, request_id, service,
                 start_time):
        self.connection = connection
        self.message_buffer = message_buffer
        self.request_id = request_id
        self.service = service
        self.start_time = start_time


class BaseWorker(StartStopMixin, with_metaclass(ABCMeta, LoopMixin)):

    Request = Request

    def __init__(self, pool_size=None):
        self.pool_size = pool_size or 10
        super(BaseWorker, self).__init__()

    def create_callback(self):
        """Create callback that should be called after request was done."""
        concurrency = self.concurrency
        acceptors = self.app.acceptors
        counter = self.app.counters['response_served']
        timeouts = self.app.timeouts
        timers = self.app.timers
        loop = self.app.loop
        delay = self.app.hub.callback

        def start_accepting():
            if not concurrency.reached:
                return
            concurrency.reached.clean()
            logger.debug('Start registered acceptors,'
                         ' current concurrency: %d...', int(concurrency))
            acceptors.start_accepting()

        def inner_callback(request, result, exception=None):
            """Process task result."""
            connection = request.connection
            request_id = request.request_id 

            if exception is None:
                success, (method, response) = True, result
                key = "{0}::{1}".format(request.service, method)
                took = loop.now() - request.start_time
                timers[key] += took
            else:
                logger.error(exception[1], exc_info=exception)
                success, response = False, ''

            if connection.is_ready():
                counter.add()
                connection.ready(success, response, request_id)
            elif exception is None and response:
                timeouts[key] += took
                logger.warn(
                    "Method %s::%s took %d ms, it's too late for %r",
                        request.service, method, int(took), connection)

            if concurrency.reached:
                delay(start_accepting)

        return inner_callback

    @abstractmethod
    def create_consumer(self):
        raise NotImplementedError()

    @cached_property
    def concurrency(self):
        """How many tasks executed in parallel?"""
        return ContextCounter()

    def create_task(self, processor):
        """Create new task for given processor."""
        concurrency = self.concurrency

        def inner_task(request):
            """Process incoming request with given processor."""
            with concurrency:
                return processor(request.message_buffer)

        return inner_task

    def create_producer(self, service):
        """Create producer for connections."""
        concurrency = self.concurrency
        pool_size = self.pool_size
        callback = self.create_callback()
        processor = self.app.services.create_processor(service)
        counter = self.app.counters['pool_overflow']
        task = self.create_task(processor)
        consume = self.create_consumer()
        acceptors = self.app.acceptors
        loop = self.app.loop
        delay = self.app.hub.callback
        Request = self.Request

        def stop_accepting():
            if concurrency.reached or pool_size > concurrency:
                return
            logger.debug('Stop registered acceptors,'
                         ' current concurrency: %d...', int(concurrency))
            counter.add()
            concurrency.reached.set()
            acceptors.stop_accepting()

        def inner_producer(connection, message_buffer, request_id):
            """Enqueue given request to thread pool."""
            request = Request(connection=connection,
                              message_buffer=message_buffer,
                              request_id=request_id,
                              service=service,
                              start_time=loop.now())
            curried_task = partial(task, request)
            consume(curried_task, partial(callback, request))
            if not concurrency.reached and pool_size <= concurrency:
                delay(stop_accepting)

        return inner_producer

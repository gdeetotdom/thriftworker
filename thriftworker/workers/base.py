from __future__ import absolute_import

import logging
from collections import namedtuple
from abc import ABCMeta, abstractmethod
from functools import partial

from six import with_metaclass

from thriftworker.utils.mixin import LoopMixin, StartStopMixin
from thriftworker.utils.atomics import ContextCounter
from thriftworker.utils.decorators import cached_property

logger = logging.getLogger(__name__)


class BaseWorker(StartStopMixin, with_metaclass(ABCMeta, LoopMixin)):

    #: Store request in this tuple.
    Request = namedtuple('Request', ('connection', 'message_buffer',
                                     'request_id', 'service',
                                     'start_time'))

    def __init__(self, pool_size=None):
        self.pool_size = pool_size or 10
        super(BaseWorker, self).__init__()

    def create_callback(self, request):
        """Create callback that should be called after request was done."""
        connection = request.connection
        request_id = request.request_id
        pool_size = self.pool_size
        concurrency = self.concurrency
        acceptors = self.app.acceptors
        counters = self.app.counters
        timers = self.app.timers
        loop = self.app.loop

        def inner_callback(result, exception=None):
            """Process task result."""
            if exception is None:
                success, (method, response) = True, result
                key = (request.service, method)
                counters[key] += 1
                timers[key] += loop.now() - request.start_time
            else:
                logger.error(exception[1], exc_info=exception)
                success, response = False, ''
            if connection.is_waiting():
                connection.ready(success, response, request_id)
            if concurrency.reached and pool_size > concurrency:
                concurrency.reached.clean()
                logger.debug('Start registered acceptors,'
                             ' current concurrency: %d...', int(concurrency))
                acceptors.start_accepting()

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
        Request = self.Request
        concurrency = self.concurrency
        pool_size = self.pool_size
        create_callback = self.create_callback
        processor = self.app.services.create_processor(service)
        counter = self.app.counters[('Internal', 'pool_overflow')]
        task = self.create_task(processor)
        consume = self.create_consumer()
        acceptors = self.app.acceptors
        loop = self.app.loop

        def inner_producer(connection, message_buffer, request_id):
            """Enqueue given request to thread pool."""
            request = Request(connection=connection,
                              message_buffer=message_buffer,
                              request_id=request_id,
                              service=service,
                              start_time=loop.now())
            curried_task = partial(task, request)
            callback = create_callback(request)
            consume(curried_task, callback)
            if not concurrency.reached and pool_size <= concurrency:
                logger.debug('Stop registered acceptors,'
                             ' current concurrency: %d...', int(concurrency))
                counter.add()
                concurrency.reached.set()
                acceptors.stop_accepting()

        return inner_producer

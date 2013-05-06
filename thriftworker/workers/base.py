from __future__ import absolute_import

import sys
import logging
from abc import ABCMeta, abstractmethod
from functools import partial

from six import with_metaclass

from ..utils.mixin import LoopMixin, StartStopMixin
from ..utils.atomics import ContextCounter
from ..utils.decorators import cached_property
from ..utils.monotime import monotonic

logger = logging.getLogger(__name__)


class Request(object):
    """Describe a request."""

    __slots__ = (
        'loop', 'connection', 'message_buffer',
        'request_id', 'service', 'receipt_time',
        'start_time', 'end_time', 'dispatch_time',
        'method', 'response', 'exception', 'successful',
    )

    def __init__(self, loop, connection, message_buffer, request_id, service):
        self.loop = loop
        self.connection = connection
        self.message_buffer = message_buffer
        self.request_id = request_id
        self.service = service
        self.receipt_time = self.loop.now()
        self.start_time = self.end_time = self.dispatch_time = None
        self.method = self.response = self.exception = None
        self.successful = None

    @property
    def dispatching_timers(self):
        return self.dispatch_time - self.receipt_time

    @property
    def execution_time(self):
        return (self.end_time - self.start_time) * 1e3

    def execute(self, processor):
        """Process our request."""
        self.start_time = monotonic()
        try:
            self.method, self.response = processor(self.message_buffer)
        except:
            successful = self.successful = False
            exception = self.exception = sys.exc_info()
            logger.error(exception[1], exc_info=exception)
        else:
            successful = self.successful = True
        finally:
            self.end_time = monotonic()
        return successful

    def dispatch(self):
        """Notify connection that request was processed."""
        self.dispatch_time = self.loop.now()
        if not self.connection.is_ready():
            return False
        self.connection.ready(self.successful, self.response, self.request_id)
        return True

    @property
    def method_name(self):
        return "{0}::{1}".format(self.service, self.method or 'unknown')


class BaseWorker(StartStopMixin, with_metaclass(ABCMeta, LoopMixin)):

    Request = Request

    def __init__(self, pool_size=None):
        self.pool_size = pool_size or 10
        super(BaseWorker, self).__init__()

    def create_callback(self):
        """Create callback that should be called after request was done."""
        concurrency = self.concurrency
        pool_size = self.pool_size
        acceptors = self.app.acceptors
        counter = self.app.counters['response_served']
        timeouts = self.app.timeouts
        execution_timers = self.app.execution_timers
        dispatching_timers = self.app.dispatching_timers
        delay = self.app.hub.callback

        def start_accepting():
            if not concurrency.reached:
                return
            concurrency.reached.clean()
            logger.info('Start registered acceptors,'
                        ' current concurrency: %d...', int(concurrency))
            acceptors.start_accepting()

        def inner_callback(request, result, exception=None):
            """Process task result."""
            method_name = request.method_name

            if request.dispatch():
                # connection is ready for answer
                counter.add()
            elif request.successful and request.response:
                # connection is not ready, we are late
                timeouts[method_name] += request.dispatching_timers
                logger.warn(
                    "Method %s took %.2f ms (exec %.2f ms), it's too late for %r",
                        method_name, request.dispatching_timers,
                        request.execution_time, request.connection)

            if request.successful:
                execution_timers[method_name] += request.execution_time
                dispatching_timers[method_name] += request.dispatching_timers

            if concurrency.reached and pool_size > concurrency:
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
                return request.execute(processor)

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
            logger.info('Stop registered acceptors,'
                        ' current concurrency: %d...', int(concurrency))
            counter.add()
            concurrency.reached.set()
            acceptors.stop_accepting()

        def inner_producer(connection, message_buffer, request_id):
            """Enqueue given request to thread pool."""
            request = Request(loop=loop,
                              connection=connection,
                              message_buffer=message_buffer,
                              request_id=request_id,
                              service=service)
            curried_task = partial(task, request)
            consume(curried_task, partial(callback, request))
            if not concurrency.reached and pool_size <= concurrency:
                delay(stop_accepting)

        return inner_producer

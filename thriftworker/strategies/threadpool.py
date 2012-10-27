"""Base pool implementation."""
from __future__ import absolute_import

from threading import Event

import zmq
from zmq import Context, Poller, ZMQError
from pyuv import Poll, UV_READABLE

from ..utils.decorators import cached_property
from ..utils.threads import DaemonThread
from ..utils.loop import in_loop
from ..queues.rr import RRQueue
from .base import BasePool


class WorkerExit(Exception):
    pass


def stop():
    raise WorkerExit()


class BaseWorker(DaemonThread):

    def __init__(self, context, socket_type):
        self.context = context
        self.socket_type = socket_type
        self.started = Event()
        self.stopped = Event()
        super(BaseWorker, self).__init__(name='{0}-{1}'.
            format(self.__class__.__name__, self.identity))

    def start(self):
        super(BaseWorker, self).start()
        self.started.wait()

    def stop(self):
        self.send(stop)
        self.stopped.wait()

    @cached_property
    def identity(self):
        return str(id(self))

    @cached_property
    def endpoint(self):
        return 'inproc://data-{0}'.format(self.identity)

    @cached_property
    def _socket(self):
        socket = self.context.socket(self.socket_type)
        socket.setsockopt(zmq.IDENTITY, self.identity)
        return socket

    @cached_property
    def _command_endpoint(self):
        return 'inproc://command-{0}'.format(self.identity)

    @cached_property
    def _pull_socket(self):
        socket = self.context.socket(zmq.PULL)
        socket.bind(self._command_endpoint)
        return socket

    @cached_property
    def _push_socket(self):
        socket = self.context.socket(zmq.PUSH)
        socket.connect(self._command_endpoint)
        return socket

    def send(self, command):
        self._push_socket.send_pyobj(command)

    @cached_property
    def _poller(self):
        poller = Poller()
        poller.register(self._socket, zmq.POLLIN)
        poller.register(self._pull_socket, zmq.POLLIN)
        return poller

    def process(self, socket):
        raise NotImplementedError('subclass responsibility')

    def after_start(self):
        pass

    def body(self):
        poller = self._poller
        data_socket = self._socket
        command_socket = self._pull_socket
        push_socket = self._push_socket
        self.after_start()

        def loop():
            """Process incoming requests until all messages are exhausted."""
            for sock, event in poller.poll():
                if sock is data_socket:
                    # Process incoming messages.
                    try:
                        while True:
                            # Exhaust incoming messages.
                            self.process(data_socket)
                    except ZMQError as exc:
                        if exc.errno != zmq.EAGAIN:
                            raise
                elif sock is command_socket:
                    # Process incoming command.
                    command = command_socket.recv_pyobj()
                    command()

        self.started.set()
        try:
            while True:
                loop()
        except WorkerExit:
            pass
        finally:
            poller.unregister(data_socket)
            poller.unregister(command_socket)
            data_socket.close()
            command_socket.close()
            push_socket.close()
            self.stopped.set()


class Worker(BaseWorker):

    def __init__(self, context, processor):
        super(Worker, self).__init__(context=context,
                                     socket_type=zmq.REP)
        self.processor = processor

    def after_start(self):
        self._socket.bind(self.endpoint)

    def process(self, socket):
        service, payload = socket.recv_multipart(flags=zmq.NOBLOCK)
        reply = self.processor(service, payload)
        socket.send(reply)


class Mediator(object):

    def __init__(self, context, loop, endpoints, identities):
        self.context = context
        self.loop = loop
        self.endpoints = endpoints
        queue = self.queue = RRQueue()
        for identity in identities:
            queue.register(identity)

    @cached_property
    def _socket(self):
        socket = self.context.socket(zmq.ROUTER)
        for endpoint in self.endpoints:
            socket.connect(endpoint)
        return socket

    @cached_property
    def _poller(self):
        poller = Poll(self.loop, self._socket.getsockopt(zmq.FD))
        poller.start(UV_READABLE, self.consume)
        return poller

    @in_loop
    def start(self):
        self._socket
        self._poller

    @in_loop
    def stop(self):
        self._poller.close()
        self._socket.close()

    def consume(self, *args, **kwargs):
        socket = self._socket
        queue = self.queue
        try:
            while True:
                reply = socket.recv_multipart(flags=zmq.NOBLOCK)
                identity = reply[0]
                payload = reply[2]
                callback = queue.pop(identity)
                callback(payload, None)
        except ZMQError as exc:
            if exc.errno != zmq.EAGAIN:
                raise

    def produce(self, request, callback):
        identity = self.queue.push(callback)
        message = (identity, '', request.service, request.data)
        self._socket.send_multipart(message)
        self.consume()


class ThreadPool(BasePool):
    """Process all request in thread pool."""

    Worker = Worker
    Mediator = Mediator

    def __init__(self, size=None):
        self.workers = set()
        self.size = size or 10
        super(ThreadPool, self).__init__()

    def start(self):
        for _ in xrange(self.size):
            worker = self.Worker(self.context, self.app.processor)
            self.workers.add(worker)
            worker.start()
        self.mediator.start()

    def stop(self):
        while self.workers:
            worker = self.workers.pop()
            worker.stop()
        self.mediator.stop()

    @cached_property
    def context(self):
        return Context.instance()

    @cached_property
    def mediator(self):
        endpoints = [worker.endpoint for worker in self.workers]
        identities = [worker.identity for worker in self.workers]
        return self.Mediator(self.context, self.app.loop,
                             endpoints, identities)

    def queue_request(self, request, callback):
        self.mediator.produce(request, callback)

"""Provide worker implementation that serve requests for multiple processors.

"""
from __future__ import absolute_import

from struct import Struct
import logging
from threading import Event

from thrift.transport.TTransport import TMemoryBuffer
from zmq.core.constants import EAGAIN, POLLIN, REP, NOBLOCK, PULL, PUSH
from zmq.core.context import ZMQError

from .constants import STATUS_FORMAT, DEFAULT_ENV, GEVENT_ENV
from .utils import cached_property, detect_environment

__all__ = ['Worker']

logger = logging.getLogger(__name__)


class Worker(object):
    """Process new requests and send response to listener."""

    app = None

    def __init__(self, processors=None):
        self.processors = {} if processors is None else processors
        self.notify_endpoint = 'inproc://notify{0}'.format(id(self))
        self.worker_endpoint = 'inproc://worker{0}'.format(id(self))
        self.formatter = Struct(STATUS_FORMAT)
        self.out_factory = self.in_factory = self.app.protocol_factory
        self.started = False
        self._started_event = Event()
        super(Worker, self).__init__()

    @cached_property
    def socket(self):
        socket = self.app.context.socket(REP)
        socket.bind(self.worker_endpoint)
        return socket

    @cached_property
    def notify_socket(self):
        socket = self.app.context.socket(PULL)
        socket.bind(self.notify_endpoint)
        return socket

    def wakeup(self):
        socket = self.app.context.socket(PUSH)
        socket.connect(self.notify_endpoint)
        socket.send('')
        socket.close()

    def wait(self):
        self._started_event.wait()

    @cached_property
    def poller(self):
        env = detect_environment()
        if env == DEFAULT_ENV:
            from zmq.core.poll import Poller
        elif env == GEVENT_ENV:
            from zmq.green import Poller
        else:
            raise NotImplementedError('Environment "{0}" not supported'
                                      .format(env))
        return Poller()

    def process(self, socket):
        request = socket.recv_multipart(flags=NOBLOCK)
        in_transport = TMemoryBuffer(request[1])
        out_transport = TMemoryBuffer()

        in_prot = self.in_factory.getProtocol(in_transport)
        out_prot = self.out_factory.getProtocol(out_transport)

        success = True
        try:
            processor = self.processors[request[0]]
            processor.process(in_prot, out_prot)
        except Exception as exc:
            logger.exception(exc)
            success = False

        socket.send_multipart((self.formatter.pack(success),
                               out_transport.getvalue()))

    def start(self):
        """Run worker."""
        self.started = True
        self.run()

    def run(self):
        """Process incoming requests."""
        assert self.started, 'worker not started'
        worker_endpoints = self.app.worker_endpoints
        process = self.process
        poller = self.poller
        notify_socket = self.notify_socket
        poller.register(notify_socket, POLLIN)
        socket = self.socket
        poller.register(socket, POLLIN)
        worker_endpoints.append(self.worker_endpoint)

        def loop():
            """Process incoming requests until all messages are exhausted."""
            for sock, event in poller.poll():
                if sock is socket:
                    # Process incoming messages.
                    try:
                        while True:
                            # Exhaust incoming messages.
                            process(socket)
                    except ZMQError as exc:
                        if exc.errno != EAGAIN:
                            raise
                elif sock is notify_socket:
                    # Receive wake-up message.
                    notify_socket.recv()

        self._started_event.set()
        try:
            while self.started:
                loop()
        finally:
            worker_endpoints.remove(self.worker_endpoint)
            poller.unregister(socket)
            poller.unregister(notify_socket)
            socket.close()
            notify_socket.close()

    def stop(self):
        """Stop worker."""
        assert self.started, 'worker not started'
        self.started = False
        self.wakeup()

"""Provide worker implementation that serve requests for multiple processors.

"""
from __future__ import absolute_import

from struct import Struct
import logging

from thrift.transport.TTransport import TMemoryBuffer
from zmq.core.constants import EAGAIN, POLLIN, REP, NOBLOCK
from zmq.core.context import ZMQError

from .constants import STATUS_FORMAT, DEFAULT_ENV, GEVENT_ENV, RCVTIMEO
from .utils import cached_property, detect_environment

__all__ = ['Worker']

logger = logging.getLogger(__name__)


class Worker(object):
    """Process new requests and send response to listener."""

    app = None

    def __init__(self, processors=None, backend_endpoint=None):
        self.processors = {} if processors is None else processors
        self.backend_endpoint = backend_endpoint or self.app.backend_endpoint
        self.formatter = Struct(STATUS_FORMAT)
        self.out_factory = self.in_factory = self.app.protocol_factory
        self.started = False
        super(Worker, self).__init__()

    @cached_property
    def socket(self):
        socket = self.app.context.socket(REP)
        socket.connect(self.backend_endpoint)
        return socket

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
        process = self.process
        poller = self.poller
        socket = self.socket
        poller.register(socket, POLLIN)

        def loop():
            """Process incoming requests until all messages are exhausted."""
            if not poller.poll(RCVTIMEO):
                # No message received.
                return
            try:
                while True:
                    # Exhaust incoming messages.
                    process(socket)
            except ZMQError as exc:
                if exc.errno != EAGAIN:
                    raise

        try:
            while self.started:
                loop()
        finally:
            poller.unregister(socket)
            socket.close()

    def stop(self):
        """Stop worker."""
        assert self.started, 'worker not started'
        self.started = False

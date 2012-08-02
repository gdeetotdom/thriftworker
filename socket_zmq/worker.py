"""Base worker implementation. Can handle request from proxy."""
from socket_zmq.utils import cached_property
from struct import Struct
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.transport.TTransport import TMemoryBuffer
from zmq.core.poll import Poller
import logging
import zmq

__all__ = ['Worker']

logger = logging.getLogger(__name__)


class Worker(object):
    """Create new worker with given processor."""

    def __init__(self, context, backend, processor):
        self.struct = Struct('!?')
        self.context = context
        self.backend = backend
        self.in_protocol = TBinaryProtocolAcceleratedFactory()
        self.out_protocol = TBinaryProtocolAcceleratedFactory()
        self.processor = processor
        self.started = None

    @cached_property
    def socket(self):
        worker_socket = self.context.socket(zmq.REP)
        worker_socket.connect(self.backend)
        return worker_socket

    @cached_property
    def poller(self):
        """Create poller for zeromq socket. We need to support clean worker 
        shutdown.

        """
        poller = Poller()
        poller.register(self.socket, zmq.POLLIN)
        return poller

    def process(self, socket):
        itransport = TMemoryBuffer(socket.recv(flags=zmq.NOBLOCK))
        otransport = TMemoryBuffer()
        iprot = self.in_protocol.getProtocol(itransport)
        oprot = self.out_protocol.getProtocol(otransport)

        try:
            self.processor.process(iprot, oprot)
        except Exception, exc:
            logger.exception(exc)
            socket.send(self.struct.pack(False), flags=zmq.SNDMORE)
            socket.send('')
        else:
            socket.send(self.struct.pack(True), flags=zmq.SNDMORE)
            socket.send(otransport.getvalue())

    def start(self):
        """Start worker. This method will block current thread until
        :meth:`stop` will be called.

        """
        self.started = True
        socket = self.socket
        try:
            while self.started:
                socks = dict(self.poller.poll(1))
                if socket in socks and socks[socket] == zmq.POLLIN:
                    try:
                        while True:
                            self.process(socket)
                    except zmq.ZMQError, exc:
                        if exc.errno != zmq.EAGAIN:
                            raise
        finally:
            socket.close()

    def stop(self):
        """Signal worker stop. Worker will stop only after processing it's
        last request.

        """
        self.started = False

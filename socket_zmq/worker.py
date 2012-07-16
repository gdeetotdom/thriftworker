from struct import Struct
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.transport.TTransport import TMemoryBuffer
import logging
import zmq

__all__ = ['Worker']


class Worker(object):

    def __init__(self, context, backend, processor):
        self.struct = Struct('!?')
        self.context = context
        self.backend = backend
        self.in_protocol = TBinaryProtocolAcceleratedFactory()
        self.out_protocol = self.in_protocol
        self.processor = processor

    def socket(self):
        worker_socket = self.context.socket(zmq.REP)
        worker_socket.connect(self.backend)
        return worker_socket

    def process(self, socket):
        itransport = TMemoryBuffer(socket.recv())
        otransport = TMemoryBuffer()
        iprot = self.in_protocol.getProtocol(itransport)
        oprot = self.out_protocol.getProtocol(otransport)

        try:
            self.processor.process(iprot, oprot)
        except Exception, exc:
            logging.exception(exc)
            socket.send(self.struct.pack(False), flags=zmq.SNDMORE)
            socket.send('')
        else:
            socket.send(self.struct.pack(True), flags=zmq.SNDMORE)
            socket.send(otransport.getvalue())

    def run(self):
        socket = self.socket()
        try:
            while True:
                self.process(socket)
        finally:
            socket.close()

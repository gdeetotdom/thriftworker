from __future__ import absolute_import

import socket
import struct
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

from thriftworker.tests.utils import TestCase
from thriftworker.transports.framed import FramedAcceptor
from thriftworker.constants import LENGTH_FORMAT, LENGTH_SIZE

from .utils import AcceptorMixin


class TestFramedAcceptor(AcceptorMixin, TestCase):

    Acceptor = FramedAcceptor

    @staticmethod
    def encode_length(data):
        return struct.pack(LENGTH_FORMAT, len(data)) + data

    @staticmethod
    def decode_length(data):
        return struct.unpack(LENGTH_FORMAT, data)[0]

    @staticmethod
    def decode_message(data):
        trans = TMemoryBuffer(data)
        proto = TBinaryProtocol(trans)
        return proto.readString()

    @staticmethod
    def create_message(data):
        trans = TMemoryBuffer()
        proto = TBinaryProtocol(trans)
        proto.writeString(data)
        return trans.getvalue()

    def test_connection(self):
        payload = b'xxxx'
        self.processor.process = lambda in_prot, out_prot: \
            out_prot.writeString(in_prot.readString())

        source = socket.socket()
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())
        with self.maybe_connect(source, acceptor) as client:
            client.send(self.encode_length(self.create_message(payload)))
            length = self.decode_length(client.recv(LENGTH_SIZE))
            decoded_payload = self.decode_message(client.recv(length))

        self.assertEqual(payload, decoded_payload)

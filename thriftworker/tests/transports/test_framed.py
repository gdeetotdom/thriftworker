from __future__ import absolute_import

import socket
import struct
from mock import Mock
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

    def test_negative_length(self):
        source = socket.socket()
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())

        with self.maybe_connect(source, acceptor) as client:
            client.send(struct.pack(LENGTH_FORMAT, -1))
            self.assertEqual('', client.recv(4))

    def test_zero_length(self):
        source = socket.socket()
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())

        with self.maybe_connect(source, acceptor) as client:
            client.send(struct.pack(LENGTH_FORMAT, 0))
            self.assertEqual('', client.recv(4))

    def test_unbuffered_client(self):
        payload = b'xxxx' * 128
        delta = 4
        self.processor.process = lambda in_prot, out_prot: \
            out_prot.writeString(in_prot.readString())

        source = socket.socket()
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())
        with self.maybe_connect(source, acceptor) as client:
            message = self.encode_length(self.create_message(payload))
            for i in xrange(len(message) // delta):
                # send only one message chunk and wake-up loop
                client.send(message[i * delta:(i + 1) * delta])
                self.wakeup_loop()
            length = self.decode_length(client.recv(LENGTH_SIZE))
            decoded_payload = self.decode_message(client.recv(length))

        self.assertEqual(payload, decoded_payload)

    def test_one_way_normal(self):
        payload1 = b'xxxx'
        payload2 = b'zzzz'
        one_way_process = Mock(return_value=None)
        echo_process = lambda in_prot, out_prot: \
            out_prot.writeString(in_prot.readString())

        source = socket.socket()
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())
        with self.maybe_connect(source, acceptor) as client:
            # send one way request
            self.processor.process = one_way_process
            client.send(self.encode_length(self.create_message(payload1)))
            self.wait_for_predicate(lambda: not one_way_process.called)

            # send play request after one-way
            self.processor.process = echo_process
            client.send(self.encode_length(self.create_message(payload2)))
            length = self.decode_length(client.recv(LENGTH_SIZE))
            decoded_payload = self.decode_message(client.recv(length))

        self.assertEqual(payload2, decoded_payload)
        self.assertTrue(one_way_process.called)
        self.assertEqual(1, one_way_process.call_count)

    def test_several_one_way(self):
        payload = b'xxxx'
        process = self.processor.process = Mock(return_value=None)
        factor = 5

        source = socket.socket()
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())
        message = self.encode_length(self.create_message(payload)) * factor
        with self.maybe_connect(source, acceptor) as client:
            client.send(message)
            self.wait_for_predicate(lambda: process.call_count != factor)

        self.assertEqual(factor, process.call_count)

    def test_exception(self):
        payload = b'xxxx'

        def process(in_prot, out_prot):
            raise Exception()

        self.processor.process = process
        source = socket.socket()
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())
        with self.maybe_connect(source, acceptor) as client:
            client.send(self.encode_length(self.create_message(payload)))
            self.assertEqual('', client.recv(4))

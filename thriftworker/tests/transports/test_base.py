from __future__ import absolute_import

import socket
from contextlib import closing

from thriftworker.tests.utils import TestCase, start_stop_ctx
from thriftworker.transports.base import BaseAcceptor, Acceptors

from .utils import AcceptorMixin, AcceptorsMixin


class EchoConnection(object):

    OPENED = 0x1
    CLOSED = 0x2

    def __init__(self, producer, loop, client, peer, on_close):
        self.state = self.OPENED
        self.producer = producer
        self.loop = loop
        self.client = client
        self.peer = peer
        self.on_close = on_close
        client.start_read(self.on_read)

    def on_read(self, handle, data, error):
        assert self.state == self.OPENED
        if not data:
            self.close()
            return
        self.client.write(data)

    def is_closed(self):
        return self.state == self.CLOSED

    def close(self):
        assert self.state == self.OPENED
        self.state = self.CLOSED
        self.client.close()
        self.on_close(self)


class Acceptor(BaseAcceptor):

    Connection = EchoConnection


class TestBaseAcceptor(AcceptorMixin, TestCase):

    Acceptor = Acceptor

    def test_connection(self):
        source = socket.socket()
        payload = b'xxxx'
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())
        with self.maybe_connect(source, acceptor) as client:
            self.assertEqual(1, acceptor.connections_number)
            client.send(payload)
            self.assertEqual(payload, client.recv(4))
        self.assertEqual(0, acceptor.connections_number)


class TestAcceptors(AcceptorsMixin, TestCase):

    Acceptor = Acceptor
    Acceptors = Acceptors

    def test_register(self):
        acceptors = self.Acceptors()
        sock = socket.socket()
        sock.bind(('localhost', 0))
        sock.listen(0)
        with closing(sock), start_stop_ctx(acceptors):
            acceptors.register(sock.fileno(), self.service_name)
            self.wakeup_loop()
            registered_acceptors = list(acceptors)
            self.assertEqual(1, len(registered_acceptors))
            acceptor = registered_acceptors[0]
            self.assertFalse(acceptor.active)
            acceptors.start_by_name(self.service_name)
            self.wakeup_loop()
            self.wait_for_predicate(lambda: not acceptor.active)
            self.assertTrue(acceptor.active)
        self.assertFalse(acceptor.active)

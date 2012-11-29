from __future__ import absolute_import

import socket
from contextlib import closing, contextmanager
from mock import Mock

from thriftworker.tests.utils import TestCase, StartStopLoopMixin, \
    start_stop_ctx
from thriftworker.transports.base import BaseAcceptor


class EchoConnection(object):

    OPENED = 0x1
    CLOSED = 0x2

    def __init__(self, producer, loop, client, sock, on_close):
        self.state = self.OPENED
        self.producer = producer
        self.loop = loop
        self.client = client
        self.sock = sock
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
        self.sock.close()
        self.on_close(self)


class Acceptor(BaseAcceptor):

    Connection = EchoConnection


class TestAcceptor(StartStopLoopMixin, TestCase):

    def setUp(self):
        super(TestAcceptor, self).setUp()
        self.Acceptor = self.app.subclass_with_self(Acceptor)
        service_name = self.service_name = 'SomeService'
        processor = self.processor = Mock()
        self.app.services.register(service_name, processor)

    @contextmanager
    def maybe_connect(self, source, acceptor):
        client = socket.socket()
        source.bind(('localhost', 0))
        source.listen(0)
        with closing(source), closing(client), start_stop_ctx(acceptor):
            client.settimeout(1.0)
            client.connect(source.getsockname())
            self.wakeup_loop()
            yield client

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

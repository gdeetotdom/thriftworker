from __future__ import absolute_import

import socket
import struct

from thriftworker.tests.utils import TestCase
from thriftworker.transports.framed import FramedAcceptor

from .utils import AcceptorMixin


class TestFramedAcceptor(AcceptorMixin, TestCase):

    Acceptor = FramedAcceptor

    def test_connection(self):
        source = socket.socket()
        payload = b'xxxx'
        acceptor = self.Acceptor(name=self.service_name,
                                 descriptor=source.fileno())
        with self.maybe_connect(source, acceptor) as client:
            client.send(struct.pack('I', len(payload)) + payload)

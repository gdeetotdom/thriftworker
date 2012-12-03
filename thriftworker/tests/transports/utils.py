from __future__ import absolute_import

import socket
from time import time, sleep
from contextlib import closing, contextmanager
from mock import Mock

from thriftworker.tests.utils import StartStopLoopMixin, start_stop_ctx


class AcceptorMixin(StartStopLoopMixin):

    Acceptor = None

    def setUp(self):
        super(AcceptorMixin, self).setUp()
        self.Acceptor = self.app.subclass_with_self(type(self).Acceptor)
        service_name = self.service_name = 'SomeService'
        processor = self.processor = Mock()
        self.app.services.register(service_name, processor)

    def wait_for_predicate(self, func, timeout=5):
        tic = time()
        while func() and tic + timeout > time():
            sleep(0.1)

    @contextmanager
    def maybe_connect(self, source, acceptor):
        client = socket.socket()
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        source.bind(('localhost', 0))
        source.listen(0)
        with closing(source), closing(client), start_stop_ctx(acceptor):
            client.settimeout(5.0)
            client.connect(source.getsockname())
            self.wakeup_loop()
            yield client

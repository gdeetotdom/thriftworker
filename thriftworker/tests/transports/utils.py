from __future__ import absolute_import

import socket
from contextlib import closing, contextmanager
from mock import Mock

from thriftworker.tests.utils import StartStopLoopMixin, start_stop_ctx

TIMEOUT = 5.0


class WorkerMixin(StartStopLoopMixin):

    def setUp(self):
        super(WorkerMixin, self).setUp()
        worker = self.app.worker
        worker.start()
        self.addCleanup(worker.stop)


class AcceptorMixin(WorkerMixin):

    Acceptor = None

    def setUp(self):
        super(AcceptorMixin, self).setUp()
        Acceptor = self.app.subclass_with_self(type(self).Acceptor)
        self.Acceptor = self.app.Acceptor = Acceptor
        service_name = self.service_name = 'SomeService'
        processor = self.processor = Mock()
        self.app.services.register(service_name, processor)

    @contextmanager
    def maybe_connect(self, source, acceptor):
        client = socket.socket()
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        source.bind(('localhost', 0))
        source.listen(0)
        with closing(source), closing(client), start_stop_ctx(acceptor):
            client.settimeout(TIMEOUT)
            client.connect(source.getsockname())
            self.wakeup_loop()
            yield client


class AcceptorsMixin(AcceptorMixin):

    Acceptors = None

    def setUp(self):
        super(AcceptorsMixin, self).setUp()
        Acceptors = self.app.subclass_with_self(type(self).Acceptors)
        self.Acceptors = self.app.Acceptors = Acceptors

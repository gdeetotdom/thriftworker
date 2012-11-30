from __future__ import absolute_import

import socket
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

from __future__ import absolute_import

from mock import Mock

from thriftworker.tests.utils import StartStopLoopMixin


class WorkerMixin(StartStopLoopMixin):

    def setUp(self):
        super(WorkerMixin, self).setUp()
        Worker = self.app.subclass_with_self(type(self).Worker)
        self.Worker = self.app.Worker = Worker
        service_name = self.service_name = 'SomeService'
        processor = self.processor = Mock()
        self.app.services.register(service_name, processor)

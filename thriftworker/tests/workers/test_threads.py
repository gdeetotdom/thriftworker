from __future__ import absolute_import

from thriftworker.workers.threads import ThreadsWorker
from thriftworker.tests.utils import TestCase

from .utils import WorkerMixin


class TestThreadsWorker(WorkerMixin, TestCase):

    Worker = ThreadsWorker

    def test_request(self):
        self.check_request(self.Worker())

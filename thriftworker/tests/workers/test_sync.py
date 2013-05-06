from __future__ import absolute_import

from thriftworker.workers.sync import SyncWorker
from thriftworker.tests.utils import TestCase

from .utils import WorkerMixin


class TestSyncWorker(WorkerMixin, TestCase):

    Worker = SyncWorker

    def test_request(self):
        self.check_request(self.Worker())

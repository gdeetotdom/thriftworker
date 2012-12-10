from __future__ import absolute_import

from thriftworker.workers.sync import SyncWorker
from thriftworker.constants import DEFAULT_ENV
from thriftworker.tests.utils import TestCase, custom_env_needed, \
    start_stop_ctx

from .utils import WorkerMixin


@custom_env_needed(DEFAULT_ENV)
class TestSyncWorker(WorkerMixin, TestCase):

    Worker = SyncWorker

    def test_start_stop(self):
        with start_stop_ctx(self.Worker()) as worker:
            pass

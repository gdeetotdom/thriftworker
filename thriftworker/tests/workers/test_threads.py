from __future__ import absolute_import

from thriftworker.workers.threads import ThreadsWorker
from thriftworker.constants import DEFAULT_ENV
from thriftworker.tests.utils import TestCase, custom_env_needed

from .utils import WorkerMixin


@custom_env_needed(DEFAULT_ENV)
class TestThreadsWorker(WorkerMixin, TestCase):

    Worker = ThreadsWorker

    def test_request(self):
        self.check_request(self.Worker())

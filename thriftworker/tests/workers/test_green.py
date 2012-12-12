from __future__ import absolute_import

try:
    from thriftworker.workers.green import GeventWorker
except ImportError:
    GeventWorker = None
from thriftworker.constants import GEVENT_ENV
from thriftworker.tests.utils import TestCase, custom_env_needed, \
    start_stop_ctx

from .utils import WorkerMixin


@custom_env_needed(GEVENT_ENV)
class TestGreenWorker(WorkerMixin, TestCase):

    Worker = GeventWorker

    def test_start_stop(self):
        with start_stop_ctx(self.Worker()) as worker:
            pass

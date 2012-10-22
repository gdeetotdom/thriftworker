"""Provide worker implementation that serve requests for multiple processors.

"""
from __future__ import absolute_import

import logging

from thrift.transport.TTransport import TMemoryBuffer

__all__ = ['Worker']

logger = logging.getLogger(__name__)


class Worker(object):
    """Process new requests and send response to listener."""

    app = None

    def __init__(self, processors=None, factory=None):
        self.processors = {} if processors is None else processors
        self.out_factory = self.in_factory = factory or self.app.protocol_factory
        super(Worker, self).__init__()

    def process(self, service, request):
        in_transport = TMemoryBuffer(request)
        out_transport = TMemoryBuffer()

        in_prot = self.in_factory.getProtocol(in_transport)
        out_prot = self.out_factory.getProtocol(out_transport)

        success = True
        try:
            processor = self.processors[service]
            processor.process(in_prot, out_prot)
        except Exception as exc:
            logger.exception(exc)
            success = False

        return success, out_transport.getvalue()

    def start(self):
        """Run worker."""
        self.app.ventilator.register(self)

    def stop(self):
        """Stop worker."""
        self.app.ventilator.remove(self)

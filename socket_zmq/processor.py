"""Implementation of request processor here.

"""
from __future__ import absolute_import

import logging
from collections import namedtuple

from thrift.transport.TTransport import TMemoryBuffer

__all__ = ['Worker']

logger = logging.getLogger(__name__)


class Processor(object):
    """Process new requests and return response."""

    app = None

    Service = namedtuple('Service', 'processor proto_factory')

    def __init__(self, processors=None):
        self.services = {}
        self.proto_factory = self.app.protocol_factory
        super(Processor, self).__init__()

    def register(self, name, processor, proto_factory=None):
        """Run worker."""
        service = self.Service(processor, proto_factory or self.proto_factory)
        self.services[name] = service

    def __call__(self, name, request):
        in_transport = TMemoryBuffer(request)
        out_transport = TMemoryBuffer()

        try:
            service = self.services[name]
        except KeyError:
            logger.error('Unknown service %r', name)
            return False, ''

        proto_factory = service.proto_factory
        in_prot = proto_factory.getProtocol(in_transport)
        out_prot = proto_factory.getProtocol(out_transport)

        try:
            service.processor.process(in_prot, out_prot)
        except Exception as exc:
            logger.exception(exc)
            return False, ''

        return True, out_transport.getvalue()

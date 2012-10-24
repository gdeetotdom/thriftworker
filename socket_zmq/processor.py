"""Implementation of request processor."""
from __future__ import absolute_import

from collections import namedtuple

from thrift.transport.TTransport import TMemoryBuffer

__all__ = ['Processor']


class Processor(object):
    """Process new requests and return response. Store processor
    for each service.

    """

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

        service = self.services[name]
        proto_factory = service.proto_factory
        in_prot = proto_factory.getProtocol(in_transport)
        out_prot = proto_factory.getProtocol(out_transport)

        service.processor.process(in_prot, out_prot)

        return out_transport.getvalue()

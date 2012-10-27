"""Store processor and protocol for each service."""
from __future__ import absolute_import

from collections import namedtuple

from thrift.transport.TTransport import TMemoryBuffer

from .exceptions import ServiceAlreadyRegistered


class Services(object):
    """Process new requests and return response. Store processor
    for each service.

    """

    app = None

    #: Holder of service processor and protocol factory.
    Service = namedtuple('Service', 'processor proto_factory')

    def __init__(self):
        self.services = {}
        self.proto_factory = self.app.protocol_factory
        super(Services, self).__init__()

    def register(self, service_name, processor, proto_factory=None):
        """Register new processor for given service."""
        service = self.Service(processor, proto_factory or self.proto_factory)
        if service_name in self.services:
            raise ServiceAlreadyRegistered('Service {0} already registered'
                                           .format(service_name))
        self.services[service_name] = service

    def create_processor(self, service_name):
        """Create function that will process incoming request and return
        payload that we should return.

        :param service_name: name of served service

        """
        service = self.services[service_name]
        proto_factory, processor = service.proto_factory, service.processor

        def inner_processor(payload):
            in_transport = TMemoryBuffer(payload)
            out_transport = TMemoryBuffer()
            in_prot = proto_factory.getProtocol(in_transport)
            out_prot = proto_factory.getProtocol(out_transport)
            processor.process(in_prot, out_prot)
            return out_transport.getvalue()

        return inner_processor

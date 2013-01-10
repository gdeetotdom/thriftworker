from __future__ import absolute_import

from thriftworker.transports.base import BaseAcceptor

from .connection import Connection


class FramedAcceptor(BaseAcceptor):

    #: Which connection should we use?
    Connection = Connection

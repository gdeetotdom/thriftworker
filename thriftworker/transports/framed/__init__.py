from __future__ import absolute_import

from thriftworker.transports.base import BaseAcceptor

from .connection import Connection


class FramedAcceptor(BaseAcceptor):

    Connection = Connection

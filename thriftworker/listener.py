from __future__ import absolute_import

import socket
import errno
import logging

from pyuv import Pipe

from .constants import BACKLOG_SIZE
from .exceptions import BindError
from .utils.mixin import LoopMixin
from .utils.loop import in_loop
from .utils.decorators import cached_property
from .utils.other import get_addresses_from_pool

logger = logging.getLogger(__name__)


class Listener(LoopMixin):

    def __init__(self, name, address, backlog=None):
        """Create new listener.

        :param name: service name
        :param address: address of socket
        :param backlog: size of socket connection queue

        """
        self.name = name
        self.address = address
        self.backlog = backlog or BACKLOG_SIZE
        super(Listener, self).__init__()

    @cached_property
    def socket(self):
        """A shortcut to create a TCP socket and bind it."""
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    @cached_property
    def channel(self):
        return Pipe(self.loop)

    @property
    def host(self):
        """Return host to which this socket is binded."""
        return self.socket.getsockname()[0]

    @property
    def port(self):
        """Return binded port number."""
        return self.socket.getsockname()[1]

    @in_loop
    def start(self):
        binded = False
        sock = self.socket
        for address in get_addresses_from_pool(self.name, self.address,
                                               self.app.port_range):
            try:
                sock.bind(address)
            except socket.error as exc:
                if exc.errno in (errno.EADDRINUSE,):
                    continue
                raise
            else:
                binded = True
                break
        if not binded:
            raise BindError("Service {0!r} can't bind to address {1!r}"
                            .format(self.name, self.address))
        self.channel.open(sock.fileno())

    @in_loop
    def stop(self):
        self.socket.close()

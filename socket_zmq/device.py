"""Wrapper around ZMQ device."""
from __future__ import absolute_import

import logging
from threading import Event

from zmq.core.context import ZMQError
from zmq.core.socket import Socket
from zmq.core.constants import ROUTER, DEALER, QUEUE, ENOTSOCK
from zmq.core import device

from .utils import cached_property, spawn

__all__ = ['Device']

logger = logging.getLogger(__name__)


class Device(object):
    """Wrapper around ZMQ device."""

    app = None

    def __init__(self, frontend_endpoint=None, backend_endpoint=None):
        self.frontend_endpoint = frontend_endpoint
        self.backend_endpoint = backend_endpoint
        self._started = Event()
        super(Device, self).__init__()

    @cached_property
    def frontend_socket(self):
        """Create frontend socket. Use always non-patched socket."""
        socket = Socket(self.app.context, ROUTER)
        socket.bind(self.frontend_endpoint)
        return socket

    @cached_property
    def backend_socket(self):
        """Create backend socket. Use always non-patched socket."""
        socket = Socket(self.app.context, DEALER)
        socket.bind(self.backend_endpoint)
        return socket

    def start(self):
        spawn(self.run)
        self._started.wait()

    def run(self):
        frontend_socket = self.frontend_socket
        backend_socket = self.backend_socket
        self._started.set()
        try:
            device(QUEUE, frontend_socket, backend_socket)
        except ZMQError as exc:
            if exc.errno != ENOTSOCK and exc.strerror != 'Context was terminated':
                raise

    def stop(self):
        self.frontend_socket.close()
        self.backend_socket.close()

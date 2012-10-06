"""Describe ZMQ endpoint."""
from logging import getLogger
from struct import Struct

cimport cython

from pyuv import Poll, UV_READABLE, UV_WRITABLE

from zmq.core.constants import (NOBLOCK, EAGAIN, FD, EVENTS, POLLIN, POLLOUT,
    RCVMORE, SNDMORE)
from zmq.core.error import ZMQError
from zmq.core.socket import Socket

from .constants import STATUS_FORMAT

__all__ = ['ZMQSink']

logger = getLogger(__name__)


cdef class ZMQSink:

    def __init__(self, object loop, object socket):
        # Default values.
        self.response = self.request = self.name = self.callback = None
        self.all_ok = True
        self.struct = Struct(STATUS_FORMAT)
        self.status = WAIT_MESSAGE

        # Given arguments.
        self.socket = socket

        # Create and start poller.
        poller = self.poller = Poll(loop, socket.getsockopt(FD))
        poller.start(UV_READABLE, self.cb_event)

    @cython.profile(False)
    cdef inline bint is_writeable(self):
        return self.status == SEND_NAME or self.status == SEND_REQUEST

    @cython.profile(False)
    cdef inline bint is_readable(self):
        return self.status == READ_REPLY or self.status == READ_STATUS

    @cython.profile(False)
    cdef inline bint is_ready(self):
        return self.status == WAIT_MESSAGE

    @cython.profile(False)
    cpdef is_closed(self):
        return self.status == CLOSED

    cdef inline void start_write_watcher(self):
        self.poller.start(UV_READABLE | UV_WRITABLE, self.cb_event)

    cdef inline void stop_write_watcher(self):
        self.poller.start(UV_READABLE, self.cb_event)

    cdef inline read(self):
        assert self.is_readable(), 'sink not readable'
        if self.status == READ_STATUS:
            self.all_ok = self.struct.unpack(self.socket.recv(NOBLOCK))[0]
            self.status = READ_REPLY

        if self.status == READ_REPLY:
            assert self.socket.getsockopt(RCVMORE), 'reply truncated'
            self.response = self.socket.recv(NOBLOCK)
            self.status = WAIT_MESSAGE

    cdef inline write(self):
        assert self.is_writeable(), 'sink not writable'
        if self.status == SEND_NAME:
            self.socket.send(self.name, NOBLOCK | SNDMORE)
            self.status = SEND_REQUEST

        if self.status == SEND_REQUEST:
            self.socket.send(self.request, NOBLOCK)
            self.request = None
            self.status = READ_STATUS

    @cython.locals(ready=cython.bint)
    cpdef close(self):
        assert not self.is_closed(), 'sink already closed'
        self.status = CLOSED
        self.poller.close()
        self.socket.close()
        self.response = self.request = self.name = self.callback = None

    cpdef ready(self, object name, object callback, object request):
        assert self.is_ready(), 'sink not ready'
        self.name = name
        self.callback = callback
        self.request = request
        self.status = SEND_NAME

        # Try to write received message.
        self.on_writable()
        if self.is_writeable():
            self.start_write_watcher()

    cdef inline on_readable(self):
        try:
            while self.is_readable():
                self.read()

        except ZMQError, exc:
            if exc.errno != EAGAIN:
                raise

        else:
            if self.is_ready() and self.callback is not None:
                self.callback(self.all_ok, self.response)
                self.all_ok = self.response = self.callback = None

    cdef inline on_writable(self):
        try:
            while self.is_writeable():
                self.write()
            self.stop_write_watcher()

        except ZMQError, exc:
            if exc.errno != EAGAIN:
                raise

        else:
            if self.is_readable():
                self.on_readable()

    cpdef cb_event(self, object poll_handle, object events, object errorno):
        try:
            if events & UV_READABLE:
                self.on_readable()
            elif events & UV_WRITABLE:
                self.on_writable()
        except Exception as exc:
            logger.exception(exc)
            self.close()

cimport cython
from logging import getLogger
from pyev import EV_ERROR
from zmq.core.constants import (NOBLOCK, EAGAIN, FD, EVENTS, POLLIN, POLLOUT,
    RCVMORE, SNDMORE)
from zmq.core.error import ZMQError
from cpython cimport bool
from zmq.core.socket cimport Socket
from socket_zmq.base cimport BaseSocket
from struct import Struct

logger = getLogger(__name__)

STATUS_FORMAT = '!?'


cdef class ZMQSink(BaseSocket):

    def __init__(self, object loop, object name, Socket socket):
        self.all_ok = self.response = self.request = self.callback = None
        self.name = name
        self.struct = Struct(STATUS_FORMAT)
        self.socket = socket
        self.status = WAIT_MESSAGE
        BaseSocket.__init__(self, loop, self.socket.getsockopt(FD))

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
    cdef inline bint is_closed(self):
        return self.status == CLOSED

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
        self.socket.close()
        self.all_ok = self.response = self.request = self.callback = None
        BaseSocket.close(self)

    cpdef ready(self, object callback, object request):
        assert self.is_ready(), 'sink not ready'
        self.callback = callback
        self.request = request
        self.status = SEND_NAME
        self.wait_writable()

    cdef inline on_readable(self):
        while self.is_readable():
            self.read()
        self.callback(self.all_ok, self.response)

    cdef inline on_writable(self):
        while self.is_writeable():
            self.write()
        if self.is_readable():
            self.wait_readable()
            self.on_readable()

    cpdef cb_io(self, object watcher, object revents):
        try:
            events = self.socket.getsockopt(EVENTS)
            if events & POLLOUT:
                self.on_writable()
            if events & POLLIN:
                self.on_readable()

        except ZMQError, exc:
            if exc.errno == EAGAIN:
                return
            self.close()
            logger.exception(exc)

        except Exception, exc:
            self.close()
            logger.exception(exc)

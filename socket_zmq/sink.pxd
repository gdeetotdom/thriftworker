from zmq.core.socket cimport Socket

from .base cimport BaseSocket


cdef enum States:
    WAIT_MESSAGE = 1
    SEND_NAME = 2
    SEND_REQUEST = 3
    READ_STATUS = 4
    READ_REPLY = 5
    CLOSED = 6


cdef class ZMQSink(BaseSocket):

    cdef object callback
    cdef Socket socket
    cdef object struct
    cdef object all_ok
    cdef object request
    cdef object response
    cdef object name
    cdef States status

    cdef inline bint is_writeable(self)
    cdef inline bint is_readable(self)
    cdef inline bint is_ready(self)
    cpdef is_closed(self)

    cdef inline read(self)
    cdef inline write(self)

    cpdef close(self)
    cpdef ready(self, object callback, object request)

    cdef inline on_readable(self)
    cdef inline on_writable(self)

    cpdef cb_readable(self, object watcher, object revents)
    cpdef cb_writable(self, object watcher, object revents)

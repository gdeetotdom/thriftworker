from .base cimport BaseSocket
from .pool cimport SinkPool
from .sink cimport ZMQSink


cdef extern from "Python.h":
    ctypedef int Py_ssize_t


cdef enum States:
    WAIT_LEN = 0
    WAIT_MESSAGE = 1
    WAIT_PROCESS = 2
    SEND_LEN = 3
    SEND_ANSWER = 4
    CLOSED = 5


cdef class Buffer:

    cdef unsigned char *handle
    cdef int length
    cdef object view

    cdef resize(self, int size)


cdef class SocketSource(BaseSocket):

    cdef object struct

    cdef States status
    cdef Py_ssize_t len
    cdef Py_ssize_t recv_bytes
    cdef Py_ssize_t sent_bytes

    cdef SinkPool pool
    cdef ZMQSink sink
    cdef object on_close
    cdef object socket
    cdef object vector_io
    cdef object address

    cdef Buffer length_buffer
    cdef Buffer buffer

    cdef inline bint is_writeable(self)
    cdef inline bint is_readable(self)
    cdef inline bint is_ready(self)
    cpdef is_closed(self)

    cdef inline read(self)
    cdef inline write(self)

    cpdef close(self)
    cpdef ready(self, object all_ok, object message)

    cdef inline on_readable(self)
    cdef inline on_writable(self)

    cpdef cb_readable(self, object watcher, object revents)
    cpdef cb_writable(self, object watcher, object revents)

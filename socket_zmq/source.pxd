from .pool cimport SinkPool
from .sink cimport ZMQSink


cdef enum States:
    WAIT_LEN = 0
    WAIT_MESSAGE = 1
    WAIT_PROCESS = 2
    SEND_ANSWER = 3
    CLOSED = 4


cdef class SocketSource:

    # Default values.
    cdef object message_length
    cdef object recv_bytes
    cdef States status
    cdef object struct
    cdef object message_buffer
    cdef object incoming_buffer

    # Given arguments.
    cdef object name
    cdef SinkPool pool
    cdef object client
    cdef object loop
    cdef object on_close

    cdef ZMQSink sink

    cdef inline bint is_writeable(self)
    cdef inline bint is_readable(self)
    cdef inline bint is_ready(self)
    cpdef is_closed(self)

    cdef inline read(self)
    cpdef ready(self, object all_ok, object message)
    cpdef close(self)

    cpdef cb_read_done(self, object handle, object data, object error)
    cpdef cb_write_done(self, object handle, object error)

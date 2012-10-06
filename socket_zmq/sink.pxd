

cdef enum States:
    WAIT_MESSAGE = 1
    SEND_NAME = 2
    SEND_REQUEST = 3
    READ_STATUS = 4
    READ_REPLY = 5
    CLOSED = 6


cdef class ZMQSink:

    cdef object write_started
    cdef object callback
    cdef object struct
    cdef object all_ok
    cdef object request
    cdef object response
    cdef object name
    cdef States status

    cdef object socket
    cdef object poller

    cdef inline bint is_writeable(self)
    cdef inline bint is_readable(self)
    cdef inline bint is_ready(self)
    cpdef is_closed(self)

    cdef inline void start_write(self)
    cdef inline void stop_write(self)

    cdef inline read(self)
    cdef inline write(self)

    cpdef close(self)
    cpdef ready(self, object name, object callback, object request)

    cdef inline on_readable(self)
    cdef inline on_writable(self)

    cpdef cb_event(self, object poll_handle, object events, object errorno)

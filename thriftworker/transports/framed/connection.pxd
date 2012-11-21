

cdef enum States:
    WAIT_LEN = 0
    WAIT_MESSAGE = 1
    WAIT_PROCESS = 2
    WAIT_ANSWER = 3
    SEND_ANSWER = 4
    CLOSED = 5


cdef class Connection:

    # Default values.
    cdef object message_length
    cdef object recv_bytes
    cdef States status
    cdef object struct
    cdef object message_buffer
    cdef object incoming_buffer

    # Given arguments.
    cdef object producer
    cdef object client
    cdef object loop
    cdef object sock
    cdef object on_close

    cdef inline bint is_writeable(self)
    cdef inline bint is_readable(self)
    cdef inline bint is_ready(self)
    cpdef is_waiting(self)
    cpdef is_closed(self)

    cdef inline read(self)
    cpdef ready(self, object all_ok, object message)
    cpdef close(self)

    cdef inline void handle_error(self, object error)
    cpdef cb_read_done(self, object handle, object data, object error)
    cpdef cb_write_done(self, object handle, object error)

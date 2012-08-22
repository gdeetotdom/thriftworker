
cdef class BaseSocket:

    cdef object fileno

    cdef object read_watcher
    cdef object write_watcher

    cdef inline void stop_read_watcher(self)
    cdef inline void start_read_watcher(self)

    cdef inline void stop_write_watcher(self)
    cdef inline void start_write_watcher(self)

    cpdef cb_readable(self, object watcher, object revents)
    cpdef cb_writable(self, object watcher, object revents)

    cpdef close(self)

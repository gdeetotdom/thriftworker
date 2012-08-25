from .pool cimport SinkPool


cdef class Proxy:

    cdef object connections
    cdef SinkPool pool
    cdef object loop
    cdef object name
    cdef object socket
    cdef object watcher
    cdef object backlog

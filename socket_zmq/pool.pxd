from zmq.core.context cimport Context

from .sink cimport ZMQSink


cdef class SinkPool(object):

    cdef int size
    cdef object loop
    cdef object pool
    cdef Context context
    cdef object worker_endpoints

    cdef inline ZMQSink create(self)
    cdef inline ZMQSink get(self)
    cdef inline put(self, ZMQSink sock)
    cpdef close(self)

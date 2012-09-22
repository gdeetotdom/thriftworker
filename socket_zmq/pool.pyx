"""Pool of ZMQ sockets."""
import cython
from collections import deque

from zmq.core.constants import REQ
from zmq.core.context import Context
from zmq.core.socket import Socket

from .sink cimport ZMQSink

__all__ = ['SinkPool']


cdef class SinkPool(object):

    def __init__(self, object loop, object context, object worker_endpoints,
                 object size):
        self.loop = loop
        self.size = size
        self.pool = deque()
        self.context = context
        self.worker_endpoints = worker_endpoints

    @cython.locals(sink=ZMQSink)
    cdef inline ZMQSink create(self):
        front_socket = self.context.socket(REQ)
        for endpoint in self.worker_endpoints:
            front_socket.connect(endpoint)
        sink = ZMQSink(self.loop, front_socket)
        return sink

    @cython.locals(sink=ZMQSink)
    cdef inline ZMQSink get(self):
        try:
            sink = self.pool.popleft()
        except IndexError:
            sink = self.create()
        return sink

    cdef inline put(self, ZMQSink sink):
        if len(self.pool) >= self.size:
            if not sink.is_closed():
                sink.close()
        else:
            self.pool.append(sink)

    @cython.locals(sink=ZMQSink)
    cpdef close(self):
        while self.pool:
            sink = self.pool.pop()
            if not sink.is_closed():
                sink.close()

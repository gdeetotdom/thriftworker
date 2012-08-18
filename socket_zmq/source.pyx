cimport cython
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

import _socket
import errno
from logging import getLogger
from struct import Struct, calcsize, pack_into

from pyev import EV_READ, EV_WRITE, EV_ERROR
from zmq.utils.buffers cimport frombuffer_2

from socket_zmq.pool cimport SinkPool
from socket_zmq.base cimport BaseSocket
from socket_zmq.sink cimport ZMQSink
from socket_zmq.vector_io import vector_io

logger = getLogger(__name__)


NONBLOCKING = {errno.EAGAIN, errno.EWOULDBLOCK}
LENGTH_FORMAT = '!i'
cdef int LENGTH_SIZE = calcsize(LENGTH_FORMAT)
cdef int BUFFER_SIZE = 4096


cdef class Buffer:

    def __cinit__(self, int size):
        self.length = size
        self.handle = <unsigned char *>PyMem_Malloc(self.length * sizeof(unsigned char))
        if self.handle == NULL:
            raise MemoryError()

    def __init__(self, int size):
        self.view = frombuffer_2(self.handle, self.length, 0)

    def __dealloc__(self):
        PyMem_Free(<void *>self.handle)

    cdef resize(self, int size):
        if self.length < size:
            self.handle = <unsigned char *>PyMem_Realloc(<void *>self.handle,
                                                         self.length * sizeof(unsigned char))
            if self.handle == NULL:
                raise MemoryError()
            self.length = size
            self.view = frombuffer_2(self.handle, self.length, 0)


cdef class SocketSource(BaseSocket):
    """Basic class is represented connection.

    It can be in state:
        WAIT_LEN --- connection is reading request length.
        WAIT_MESSAGE --- connection is reading request.
        WAIT_PROCESS --- connection has just read whole request and
            waits for call ready routine.
        SEND_ANSWER --- connection is sending answer string (including length
            of answer).
        CLOSED --- socket was closed and connection should be deleted.

    """

    def __init__(self, SinkPool pool, object loop, object socket,
                 object address, object on_close):

        self.sent_bytes = self.recv_bytes = self.len = 0
        self.status = WAIT_LEN

        self.struct = Struct(LENGTH_FORMAT)

        self.length_buffer = Buffer(LENGTH_SIZE)
        self.buffer = Buffer(BUFFER_SIZE)

        self.address = address
        self.on_close = on_close
        self.socket = socket
        self.vector_io = vector_io(socket.family, socket.fileno())
        self.pool = pool
        self.sink = self.pool.get()

        BaseSocket.__init__(self, loop, self.socket.fileno())

    @cython.profile(False)
    cdef inline bint is_writeable(self):
        """Returns ``True`` if source is writable."""
        return self.status == SEND_LEN or self.status == SEND_ANSWER

    @cython.profile(False)
    cdef inline bint is_readable(self):
        """Returns ``True`` if source is readable."""
        return self.status == WAIT_LEN or self.status == WAIT_MESSAGE

    @cython.profile(False)
    cdef inline bint is_closed(self):
        """Returns ``True`` if source is closed."""
        return self.status == CLOSED

    @cython.profile(False)
    cdef inline bint is_ready(self):
        """Returns ``True`` if source is ready."""
        return self.status == WAIT_PROCESS

    @cython.locals(received=cython.int, message_length=cython.int)
    cdef inline read(self):
        """Reads data from stream and switch state."""
        assert self.is_readable(), 'socket in non-readable state'
        received = 0

        if self.status == WAIT_LEN:
            received = self.vector_io.recvmsg_into((self.length_buffer.view,
                                                    self.buffer.view))[0]
            if received == 0:
                # if we read 0 bytes and message is empty, it means client
                # close connection
                self.close()
                return
            assert received >= LENGTH_SIZE, "message length can't be read"

            message_length = self.struct.unpack_from(self.length_buffer.view)[0]
            assert message_length > 0, "negative or empty frame size, it seems" \
                                       " client doesn't use FramedTransport"

            self.buffer.resize(message_length)
            self.len = message_length
            self.status = WAIT_MESSAGE

            received = received - LENGTH_SIZE

        elif self.status == WAIT_MESSAGE:
            received = self.socket.recv_into(self.buffer.view[self.recv_bytes:self.len],
                                             self.len - self.recv_bytes)

        assert received > 0, "can't read frame from socket"

        self.recv_bytes += received
        if self.recv_bytes == self.len:
            self.recv_bytes = 0
            self.status = WAIT_PROCESS

    @cython.locals(written=cython.int)
    cdef inline write(self):
        """Writes data from socket and switch state."""
        assert self.is_writeable(), 'socket in non writable state'
        written = 0

        if self.status == SEND_LEN:
            written = self.vector_io.sendmsg((self.length_buffer.view,
                                              self.buffer.view[0:self.len]))
            assert written >= LENGTH_SIZE, "message length can't be written"
            written = written - LENGTH_SIZE
            self.status = SEND_ANSWER

        elif self.status == SEND_ANSWER:
            written = self.socket.send(self.buffer.view[self.sent_bytes:
                                                        self.len - self.sent_bytes])

        self.sent_bytes += written
        if self.sent_bytes == self.len:
            self.status = WAIT_LEN
            self.len = 0
            self.sent_bytes = 0

    cpdef close(self):
        """Closes connection."""
        assert not self.is_closed(), 'socket already closed'

        # close socket
        self.status = CLOSED
        self.socket.close()
        self.vector_io = self.socket = None

        # close sink if needed
        if self.sink.is_ready():
            # sink is ready, return to pool
            self.pool.put(self.sink)
        elif not self.sink.is_closed():
            # sink is not closed, close it
            self.sink.close()
        self.pool = self.sink = None

        # execute callback
        self.on_close(self)
        self.on_close = None

        # remove objects
        self.length_buffer = self.buffer = None

        BaseSocket.close(self)

    @cython.locals(message_length=cython.int)
    cpdef ready(self, object all_ok, object message):
        """The ready can switch Connection to three states:

            WAIT_LEN if request was oneway.
            SEND_ANSWER if request was processed in normal way.
            CLOSED if request throws unexpected exception.

        """
        assert self.is_ready(), 'socket is not ready'

        if not all_ok:
            self.close()
            return

        message_length = len(message)
        self.len = message_length

        if message_length == 0:
            # it was a oneway request, do not write answer
            self.status = WAIT_LEN
        else:
            # resize buffer if needed
            self.buffer.resize(self.len)
            # pack message size
            self.struct.pack_into(self.length_buffer.view, 0, message_length)
            # copy message to buffer
            self.buffer.view[0:self.len] = message
            self.status = SEND_LEN
            self.wait_writable()

    cdef inline on_readable(self):
        while self.is_readable():
            self.read()
        if self.is_ready():
            self.sink.ready(self.ready, self.buffer.view[0:self.len])

    cdef inline on_writable(self):
        while self.is_writeable():
            self.write()
        if self.is_readable():
            self.wait_readable()

    cpdef cb_io(self, object watcher, object revents):
        try:
            if revents & EV_WRITE:
                self.on_writable()
            elif revents & EV_READ:
                self.on_readable()

        except _socket.error, exc:
            if exc.errno in NONBLOCKING:
                # socket can't be processed now, return
                return
            logger.error(exc, exc_info=1, extra={'host': self.address[0],
                                                 'port': self.address[1]})
            self.close()

        except Exception, exc:
            logger.error(exc, exc_info=1, extra={'host': self.address[0],
                                                 'port': self.address[1]})
            self.close()

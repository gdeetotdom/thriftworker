import _socket
from logging import getLogger
from struct import Struct

cimport cython
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from pyev import EV_READ, EV_WRITE, EV_ERROR

from .constants import LENGTH_FORMAT, LENGTH_SIZE, BUFFER_SIZE, NONBLOCKING
from .buffers cimport frombuffer_2
from .pool cimport SinkPool
from .base cimport BaseSocket
from .sink cimport ZMQSink
from .vector_io import vector_io

__all__ = ['SocketSource']

logger = getLogger(__name__)


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

    def __init__(self, object name, SinkPool pool, object loop, object socket,
                 object on_close):
        self.name = name

        self.sent_bytes = self.recv_bytes = self.len = 0
        self.status = WAIT_LEN

        self.struct = Struct(LENGTH_FORMAT)

        self.length_buffer = Buffer(LENGTH_SIZE)
        self.buffer = Buffer(BUFFER_SIZE)

        self.on_close = on_close
        self.socket = socket
        self.vector_io = vector_io(socket.family, socket.fileno())
        self.pool = pool
        self.sink = self.pool.get()

        BaseSocket.__init__(self, loop, self.socket.fileno())

        self.start_read_watcher()

    @cython.profile(False)
    cdef inline bint is_writeable(self):
        """Returns ``True`` if source is writable."""
        return self.status == SEND_LEN or self.status == SEND_ANSWER

    @cython.profile(False)
    cdef inline bint is_readable(self):
        """Returns ``True`` if source is readable."""
        return self.status == WAIT_LEN or self.status == WAIT_MESSAGE

    @cython.profile(False)
    cdef inline bint is_ready(self):
        """Returns ``True`` if source is ready."""
        return self.status == WAIT_PROCESS

    @cython.profile(False)
    cpdef is_closed(self):
        """Returns ``True`` if source is closed."""
        return self.status == CLOSED

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

            assert received > LENGTH_SIZE, "message length can't be read"

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

            if received == 0:
                # if we read 0 bytes and message is empty, it means client
                # close connection
                self.close()
                return

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

        message_length = self.len = len(message)

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

            # Try to write message right now.
            self.on_writable()
            if self.is_writeable():
                self.start_write_watcher()

    cdef inline on_readable(self):
        try:
            while self.is_readable():
                self.read()

        except _socket.error, exc:
            if exc.errno not in NONBLOCKING:
                raise

        else:
            if self.is_ready() and self.sink.is_ready():
                self.sink.ready(self.name, self.ready, self.buffer.view[0:self.len])

            elif self.is_ready():
                self.close()

    cdef inline on_writable(self):
        try:
            while self.is_writeable():
                self.write()
            self.stop_write_watcher()

        except _socket.error, exc:
            if exc.errno not in NONBLOCKING:
                raise

    cpdef cb_readable(self, object watcher, object revents):
        try:
            self.on_readable()
        except Exception as exc:
            logger.exception(exc)
            self.close()

    cpdef cb_writable(self, object watcher, object revents):
        try:
            self.on_writable()
        except Exception as exc:
            logger.exception(exc)
            self.close()

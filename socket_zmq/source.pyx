from logging import getLogger
from struct import Struct
from io import BytesIO

cimport cython

from pyuv.errno import strerror

from .constants import LENGTH_FORMAT, LENGTH_SIZE, BUFFER_SIZE, NONBLOCKING
from .pool cimport SinkPool
from .sink cimport ZMQSink

__all__ = ['SocketSource']

logger = getLogger(__name__)


cdef class SocketSource:
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

    def __init__(self, object name, SinkPool pool, object loop, object client,
                 object on_close):
        # Default values.
        self.recv_bytes = self.message_length = 0
        self.status = WAIT_LEN
        self.struct = Struct(LENGTH_FORMAT)
        self.message_buffer = BytesIO()
        self.incoming_buffer = None

        # Given arguments.
        self.name = name
        self.pool = pool
        self.loop = loop
        self.client = client
        self.on_close = on_close

        self.sink = self.pool.get()

        # Start watchers.
        self.client.start_read(self.cb_read_done)

    @cython.profile(False)
    cdef inline bint is_writeable(self):
        """Returns ``True`` if source is writable."""
        return self.status == SEND_ANSWER

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

    cdef inline read(self):
        """Reads data from stream and switch state."""
        assert self.is_readable(), 'socket in non-readable state'
        received = len(self.incoming_buffer)

        if received == 0:
            # if we read 0 bytes and message is empty, it means client
            # close connection
            self.close()
            return

        if self.status == WAIT_LEN:
            assert received > LENGTH_SIZE, "message length can't be read"

            view = buffer(self.incoming_buffer)
            message_length = self.struct.unpack_from(view[0:LENGTH_SIZE])[0]
            assert message_length > 0, "negative or empty frame size, it seems" \
                                       " client doesn't use FramedTransport"

            self.message_buffer.write(view[LENGTH_SIZE:])
            self.message_length = message_length
            self.status = WAIT_MESSAGE

            received = received - LENGTH_SIZE

        elif self.status == WAIT_MESSAGE:
            # Simply write data to buffer.
            self.message_buffer.write(self.incoming_buffer)

        self.recv_bytes += received
        if self.recv_bytes == self.message_length:
            self.recv_bytes = 0
            self.status = WAIT_PROCESS

    cpdef close(self):
        """Closes connection."""
        assert not self.is_closed(), 'socket already closed'

        # Close socket.
        self.status = CLOSED
        self.client.close()

        # Close sink if needed.
        if self.sink.is_ready():
            # Sink is ready, return to pool.
            self.pool.put(self.sink)

        elif not self.sink.is_closed():
            # Sink is not closed, close it.
            self.sink.close()

        # Execute callback.
        self.on_close(self)

        # Remove references to objects.
        self.on_close = self.pool = self.sink = None
        self.client = self.message_buffer = self.incoming_buffer = None

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

        message_length = self.message_length = len(message)

        if message_length == 0:
            # it was a oneway request, do not write answer
            self.status = WAIT_LEN
        else:
            # Create buffer for message.
            view = memoryview(bytearray(message_length + LENGTH_SIZE))
            # Create message.
            view[:LENGTH_SIZE] = self.struct.pack(message_length)
            view[LENGTH_SIZE:] = message
            self.status = SEND_ANSWER
            # Write data.
            self.client.write(view, self.cb_write_done)

    cpdef cb_read_done(self, object handle, object data, object error):
        if error is not None:
            logger.error(strerror(error))
            self.close()
            return

        try:
            self.incoming_buffer = data
            while self.is_readable():
                # Try to read whole message to buffer while we can.
                self.read()

            if not self.is_ready():
                # We aren't ready to transfer message to sink.
                return

            if self.sink.is_ready():
                # Message is ready to transfer to sink.
                message_buffer = self.message_buffer
                self.sink.ready(self.name, self.ready, message_buffer.getvalue())
                # Reset message buffer.
                message_buffer.truncate()
                message_buffer.seek(0)
            else:
                # Socket was closed while we wait for sink reply.
                self.close()

        except Exception as exc:
            logger.exception(exc)
            self.close()

    cpdef cb_write_done(self, object handle, object error):
        assert self.is_writeable(), 'socket in non writable state'
        if error is not None:
            logger.error(strerror(error))
            self.close()
            return

        self.status = WAIT_LEN

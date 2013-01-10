from logging import getLogger
from struct import Struct
from sys import maxint

cimport cython
from six import next
from cStringIO import StringIO
from pyuv.errno import strerror, UV_EOF

from thriftworker.constants import LENGTH_FORMAT, LENGTH_SIZE, NONBLOCKING

logger = getLogger(__name__)


cdef class Connection:
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

    def __init__(self, object producer, object loop, object client,
                 object sock, object on_close):
        # Default values.
        self.left_buffer = None
        self.recv_bytes = self.message_length = 0
        self.status = WAIT_LEN
        self.struct = Struct(LENGTH_FORMAT)
        self.message_buffer = StringIO()
        self.incoming_buffer = None

        # Create request id generator.
        self.request_id = 0
        self.request_id_generator = iter(xrange(maxint // 2))

        # Given arguments.
        self.producer = producer
        self.loop = loop
        self.client = client
        self.sock = sock
        self.peer = sock.getpeername()
        self.on_close = on_close

        # Start watchers.
        self.client.start_read(self.cb_read_done)

    @cython.profile(False)
    cdef inline object next_request_id(self):
        """Returns ``True`` if source is writable."""
        try:
            request_id = self.request_id = next(self.request_id_generator)
        except StopIteration:
            generator = self.request_id_generator = iter(xrange(maxint // 2))
            request_id = self.request_id = next(generator)
        return request_id

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
    cpdef is_waiting(self):
        """Returns ``True`` if source is waiting for answer."""
        return self.status == WAIT_ANSWER

    @cython.profile(False)
    cpdef is_closed(self):
        """Returns ``True`` if source is closed."""
        return self.status == CLOSED

    cdef inline read(self):
        """Reads data from stream and switch state."""
        assert self.is_readable(), 'socket in non-readable state'
        incoming_buffer = self.incoming_buffer
        received = len(incoming_buffer)
        self.incoming_buffer = ''

        if received == 0:
            # if we read 0 bytes and message is empty, it means client
            # close connection
            self.close()
            return

        if self.status == WAIT_LEN:
            assert received >= LENGTH_SIZE, "message length can't be read"

            view = buffer(incoming_buffer)
            message_length = self.struct.unpack_from(view[0:LENGTH_SIZE])[0]
            assert message_length > 0, "negative or empty frame size, it seems" \
                                       " client doesn't use FramedTransport"

            self.message_buffer.write(view[LENGTH_SIZE:message_length + LENGTH_SIZE])
            self.message_length = message_length
            self.status = WAIT_MESSAGE

            received = received - LENGTH_SIZE

        elif self.status == WAIT_MESSAGE:
            # Simply write data to buffer.
            left = self.message_length - self.recv_bytes
            self.message_buffer.write(incoming_buffer[:left])

        self.recv_bytes += received
        recv_bytes = self.recv_bytes

        # We receive whole message.
        if recv_bytes >= self.message_length:
            self.recv_bytes = 0
            self.status = WAIT_PROCESS

        # Two requested come together.
        if recv_bytes > self.message_length:
            since = self.message_length - recv_bytes
            self.left_buffer = incoming_buffer[since:]

    cpdef close(self):
        """Closes connection."""
        assert not self.is_closed(), 'socket already closed'

        # Close socket.
        self.status = CLOSED
        self.client.close()
        self.sock.close()

        # Execute callback.
        self.on_close(self)

        # Remove references to objects.
        self.on_close = self.client = self.message_buffer = \
            self.incoming_buffer = None

    cpdef ready(self, object all_ok, object message, object request_id):
        """The ready can switch Connection to three states:

            WAIT_LEN if request was oneway.
            SEND_ANSWER if request was processed in normal way.
            CLOSED if request throws unexpected exception.

        """
        assert self.is_waiting(), 'socket is not waiting for answer'

        if self.request_id != request_id:
            return

        if not all_ok:
            self.close()
            return

        message_length = self.message_length = len(message)

        if message_length == 0:
            # it was a oneway request, do not write answer
            self.status = WAIT_LEN
        else:
            # Create message.
            data = self.struct.pack(message_length)
            data += message
            self.status = SEND_ANSWER
            # Write data.
            self.client.write(data, self.cb_write_done)

    cdef inline void handle_error(self, object error):
        logger.error('Error from %s: %s', "{0[0]}:{0[1]}".format(self.peer),
                     strerror(error))

    cpdef cb_read_done(self, object handle, object data, object error):
        if error:
            if error != UV_EOF:
                self.handle_error(error)
            self.close()
            return

        try:
            self.incoming_buffer = data or ''
            if self.is_readable():
                # Try to read whole message to buffer while we can.
                self.read()

            if not self.is_ready():
                # We aren't ready to transfer message to sink.
                return
            elif not self.is_waiting():
                # Grow up request id.
                request_id = self.next_request_id()
                # Change state to needed.
                self.status = WAIT_ANSWER if not self.left_buffer else WAIT_LEN
                # Send message to workers.
                self.producer(self, self.message_buffer, request_id)
                # Reset message buffer.
                self.message_buffer = StringIO()
            else:
                # Socket was closed while we wait for answer.
                self.close()

            if self.left_buffer:
                # If something left in buffer that's because of one way
                # request. We may to ignore answer to previous request.
                left_buffer = self.left_buffer
                self.left_buffer = None
                self.cb_read_done(handle, left_buffer, error)

        except Exception as exc:
            logger.exception(exc)
            self.close()

    cpdef cb_write_done(self, object handle, object error):
        assert self.is_writeable(), 'socket in non writable state'

        if error:
            self.handle_error(error)
            self.close()
            return

        self.status = WAIT_LEN

    def __repr__(self):
        return ('<{0} from {2[0]}:{2[1]} at {1}>'.
                format(self.__class__.__name__, hex(id(self)), self.peer))

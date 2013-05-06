from io import BytesIO
from logging import getLogger
from struct import Struct

cimport cython
from pyuv.errno import strerror, UV_EOF

from thriftworker.utils.stats import Counter
from thriftworker.constants import LENGTH_FORMAT, LENGTH_SIZE

logger = getLogger(__name__)


cdef object length_struct = Struct(LENGTH_FORMAT)


cdef enum ReadState:
    READ_LEN = 0
    READ_PAYLOAD = 1
    READ_DONE = 2


cdef enum ConnectionState:
    CONNECTION_READY = 0
    CONNECTION_CLOSED = 1


cdef class InputPacket:
    """Represent some framed packet that we can read."""

    # Number of input packet.
    cdef int packet_id

    # Length of message.
    cdef int length

    # Number of received bytes.
    cdef int received

    # Current state of packet.
    cdef ReadState state

    # Buffer for packet payload.
    cdef object payload

    def __init__(self, packet_id):
        self.packet_id = packet_id
        self.length = 0
        self.state = READ_LEN
        self.payload = BytesIO()

    cdef inline bint is_ready(self):
        """Returns ``True`` if packet is received."""
        return self.state == READ_DONE

    cdef inline object read_length(self, object incoming):
        """Get length from message and return relative position."""
        assert self.state == READ_LEN, 'too late for length'
        assert len(incoming) >= LENGTH_SIZE, "packet length can't be read"

        self.length = length_struct.unpack_from(incoming[0:LENGTH_SIZE].tobytes())[0]
        assert self.length > 0, "negative or empty frame size, it seems" \
                                " client doesn't use FramedTransport"

        self.state = READ_PAYLOAD
        return LENGTH_SIZE

    cdef inline object read_payload(self, object incoming):
        """Reads data from stream and switch state."""
        assert self.state == READ_PAYLOAD, 'too early or too late for payload'

        incoming_length = len(incoming)
        self.received += incoming_length
        self.payload.write(incoming[:self.length])

        if self.received >= self.length:
            self.state = READ_DONE
            return self.length
        else:
            return incoming_length

    cdef object push(self, object incoming):
        """Process incoming bytes."""
        cdef int position = 0
        cdef object view = memoryview(incoming)
        while view:
            if self.state == READ_LEN:
                position = self.read_length(view)
            elif self.state == READ_PAYLOAD:
                position = self.read_payload(view)
            else:
                return view[position:].tobytes()
            view = view[position:]
            position = 0
        return ''

    cdef inline object get_buffer(self):
        """Return packet value."""
        assert self.state == READ_DONE, 'packet not received'
        return self.payload


cdef class Connection:
    """Connection that work with framed packets."""

    # Store id of next packet.
    cdef int next_packet_id

    # Store id of current packet.
    cdef int current_packet_id

    # Store current packet here.
    cdef InputPacket current_packet

    # Current state of connection.
    cdef ConnectionState state

    # Remote peer name.
    cdef object peer

    cdef object producer
    cdef object handle
    cdef object close_callback

    def __init__(self, object producer, object loop, object handle, object peer, object close_callback):
        # Default variables.
        self.next_packet_id = 0
        self.current_packet_id = 0
        self.current_packet = self.create_packet()
        self.state = CONNECTION_READY

        # Given arguments.
        self.producer = producer
        self.handle = handle
        self.peer = peer
        self.close_callback = close_callback

        # Start watchers.
        self.handle.start_read(self.cb_read_done)

    cdef inline InputPacket create_packet(self):
        """Create new packet for processing."""
        self.next_packet_id += 1
        return InputPacket(self.next_packet_id)

    cpdef object is_ready(self):
        """Returns ``True`` if connection is ready."""
        return self.state == CONNECTION_READY

    cpdef object is_closed(self):
        """Returns ``True`` if connection is closed."""
        return self.state == CONNECTION_CLOSED

    def on_close(self, handle):
        if self.close_callback is not None:
            try:
                self.close_callback(self)
            finally:
                # Remove references to callback.
                self.close_callback = None

    def on_shutdown(self, handle, error):
        if error:
            self.handle_error(error)
        if self.handle.closed:
            self.on_close(handle)
        else:
            self.handle.close(self.on_close)

    def close(self):
        """Closes connection."""
        assert not self.is_closed(), 'connection already closed'
        self.state = CONNECTION_CLOSED
        self.handle.shutdown(self.on_shutdown)

    def ready(self, object all_ok, object data, int packet_id):
        assert self.is_ready(), 'connection not ready'

        if self.current_packet_id != packet_id:
            return

        if not all_ok:
            self.close()
            return

        cdef int data_length = len(data)
        if data_length != 0:
            # Prepend length to message
            data = length_struct.pack(data_length) + data
            self.handle.write(data, self.cb_write_done)

    cdef inline void handle_error(self, object error):
        logger.warn('Error with %r: %s', self, strerror(error))

    def cb_read_done(self, object handle, object data, object error):
        if error:
            if error != UV_EOF:
                self.handle_error(error)
            self.close()
            return

        if not data:
            # if message is empty, it means that client close connection
            self.close()
            return

        cdef int packet_id = 0
        cdef InputPacket packet = self.current_packet
        try:
            while data:
                data = packet.push(data)
                if packet.is_ready():
                    packet_id = self.current_packet_id = packet.packet_id
                    self.producer(self, packet.get_buffer(), packet_id)
                    packet = self.current_packet = self.create_packet()

        except Exception as exc:
            logger.exception(exc)
            self.close()

    def cb_write_done(self, object handle, object error):
        if error:
            self.handle_error(error)
            self.close()

    def __repr__(self):
        return ('<{0} from {1[0]}:{1[1]}>'.format(type(self).__name__, self.peer))

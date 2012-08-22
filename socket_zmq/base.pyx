"""Contains class:`BaseSocket`."""
from pyev import Io, EV_READ, EV_WRITE


cdef class BaseSocket:
    """Base class for sources and sinks."""

    def __init__(self, object loop, object fileno):
        self.fileno = fileno
        self.read_watcher = Io(self.fileno, EV_READ, loop, self.cb_readable)
        self.write_watcher = Io(self.fileno, EV_WRITE, loop, self.cb_writable)

    cdef inline void stop_read_watcher(self):
        """Stop read watcher."""
        self.read_watcher.stop()

    cdef inline void start_read_watcher(self):
        """Start read watcher."""
        self.read_watcher.start()

    cdef inline void stop_write_watcher(self):
        """Stop write watcher."""
        self.write_watcher.stop()

    cdef inline void start_write_watcher(self):
        """Start write watcher."""
        self.write_watcher.start()

    cpdef cb_readable(self, object watcher, object revents):
        """Called when file descriptor become readable."""
        raise NotImplementedError()

    cpdef cb_writable(self, object watcher, object revents):
        """Called when file descriptor become writable."""
        raise NotImplementedError()

    cpdef close(self):
        """Closes and unset watchers."""
        self.stop_read_watcher()
        self.start_write_watcher()
        self.read_watcher = self.write_watcher = None

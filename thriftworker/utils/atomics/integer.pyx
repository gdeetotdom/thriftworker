"""Some wrappers around sync."""
from libc.stdint cimport int64_t

from .boolean import AtomicBoolean


cdef extern from "stdbool.h":

    ctypedef unsigned short bool


cdef extern from *:
    bool __sync_bool_compare_and_swap(int64_t *ptr, int64_t oldval, int64_t newval)
    int64_t __sync_add_and_fetch(int64_t *ptr, int64_t val)
    int64_t __sync_sub_and_fetch(int64_t *ptr, int64_t val)


cdef class AtomicInteger(object):
    """An int value that update atomically."""

    cdef int64_t _val

    def __init__(self, val=None):
        self._val = int(val or 0)

    cpdef int64_t get(self):
        """Return current value of integer."""
        return __sync_add_and_fetch(&self._val, 0)

    def __int__(self):
        return self.get()

    cpdef int64_t set(self, int64_t val):
        """Atomically set new value and return it."""
        while not __sync_bool_compare_and_swap(&self._val, self._val, val):
            pass
        return self._val

    property value:

        def __get__(self):
            return self.get()

        def __set__(self, val):
            self.set(val)

    cpdef add(self, int64_t val):
        """Atomically add given value to the current and return it."""
        val = self._val = __sync_add_and_fetch(&self._val, val)
        return val

    def __iadd__(self, val):
        self.add(val)
        return self

    cpdef sub(self, int64_t val):
        """Atomically subtract given value from the current and return it."""
        val = self._val = __sync_sub_and_fetch(&self._val, val)
        return val

    def __isub__(self, val):
        self.sub(val)
        return self

    def __repr__(self):
        return ('<{0}({2.value}) at {1}>'.
                format(self.__class__.__name__, hex(id(self)), self))

    cpdef incr(self):
        """Atomically increment integer and return value."""
        return self.add(1)

    cpdef decr(self):
        """Atomically decrement integer and return value."""
        return self.sub(1)

    def __richcmp__(self, other, int op):
        cdef int64_t a = self.get()
        cdef int64_t b = int(other)
        if op == 0:
            # <
            return a < b
        elif op == 2:
            # ==
            return a == b
        elif op == 4:
            # >
            return a > b
        elif op == 1:
            # <=
            return a <= b
        elif op == 3:
            # !=
            return a != b
        elif op == 5:
            # >=
            return a >= b
        return False


cdef class ContextCounter(AtomicInteger):
    """Count currently executing context."""

    cdef readonly int64_t limit
    cdef readonly object reached

    def __init__(self):
        self.reached = AtomicBoolean(False)
        AtomicInteger.__init__(self)

    def __enter__(self):
        return self.incr()

    def __exit__(self, type, value, tb):
        self.decr()

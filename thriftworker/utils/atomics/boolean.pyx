"""Some wrappers around sync."""
from libc.stdint cimport uint_fast8_t
from cpython.bool cimport PyBool_FromLong, PyBool_Check, bool


cdef extern from "stdbool.h":

    ctypedef unsigned short cbool "bool"


cdef extern from *:
    cbool __sync_bool_compare_and_swap(uint_fast8_t *ptr, uint_fast8_t oldval, uint_fast8_t newval)
    uint_fast8_t __sync_add_and_fetch(uint_fast8_t *ptr, uint_fast8_t val)
    uint_fast8_t __sync_sub_and_fetch(uint_fast8_t *ptr, uint_fast8_t val)


cdef class AtomicBoolean(object):
    """An boolean value that update atomically."""

    cdef uint_fast8_t _val

    def __init__(self, val=False):
        self._val = 1 if val else 0

    cpdef bool get(self):
        """Return current value."""
        return PyBool_FromLong(__sync_add_and_fetch(&self._val, 0))

    def __nonzero__(self):
        return self.get()

    cpdef bool set(self, bool val=True):
        """Atomically set new value (True by default) and return it."""
        cdef uint_fast8_t _val = val
        while not __sync_bool_compare_and_swap(&self._val, self._val, _val):
            pass
        return PyBool_FromLong(self._val)

    cpdef bool clean(self):
        """Set boolean to False."""
        return self.set(False)

    property value:
        """Property-like access to boolean."""

        def __get__(self):
            return self.get()

        def __set__(self, val):
            self.set(val)

    def __repr__(self):
        return ('<{0}({2.value}) at {1}>'.
                format(self.__class__.__name__, hex(id(self)), self))

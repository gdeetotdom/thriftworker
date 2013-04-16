

cdef extern from "monotonic.h":
    double monotonic()


cpdef double monotonic_time():
    cdef double r = monotonic()
    if r < 0:
        raise OSError()
    return r

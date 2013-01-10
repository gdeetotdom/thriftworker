from libc.stdlib cimport malloc, free


cdef extern from "stdint.h" nogil:

    ctypedef unsigned int uint64_t


cdef extern from "cm_counter.h":

    ctypedef struct counter:
        uint64_t count
        double sum
        double squared_sum
        double min
        double max

    int init_counter(counter* c_counter)
    int counter_add_sample(counter *c_counter, double sample)
    uint64_t counter_count(counter *c_counter)
    double counter_mean(counter *c_counter)
    double counter_stddev(counter *c_counter)
    double counter_sum(counter *c_counter)
    double counter_squared_sum(counter *c_counter)
    double counter_min(counter *c_counter)
    double counter_max(counter *c_counter)


cdef class Counter(object):
    cdef counter *_c_counter

    def __cinit__(self):
        self._c_counter = <counter *>malloc(sizeof(counter))
        if self._c_counter is NULL:
            raise MemoryError()
        assert init_counter(self._c_counter) == 0

    def __dealloc__(self):
        if self._c_counter is not NULL:
            free(self._c_counter)

    cpdef add(self, double sample=1.0):
        assert counter_add_sample(self._c_counter, sample) == 0

    def __int__(self):
        return int(self.sum)

    def __long__(self):
        return long(self.sum)

    def __float__(self):
        return float(self.sum)

    def __len__(self):
        return self.count

    def __iadd__(self, double sample):
        self.add(sample)
        return self

    def __cmp__(self, other):
        if self.sum < other.sum:
            return -1
        elif self.sum == other.sum:
            return 0
        else:
            return 1

    def __repr__(self):
        return ('<{0}(count={2.count}, sum={2.sum}) at {1}>'.
                format(self.__class__.__name__, hex(id(self)), self))

    property mean:

        def __get__(self):
            return counter_mean(self._c_counter)

    property stddev:

        def __get__(self):
            return counter_stddev(self._c_counter)

    property sum:

        def __get__(self):
            return counter_sum(self._c_counter)

    property count:

        def __get__(self):
            return counter_count(self._c_counter)

    property squared_sum:

        def __get__(self):
            return counter_squared_sum(self._c_counter)

    property min:

        def __get__(self):
            return counter_min(self._c_counter)

    property max:

        def __get__(self):
            return counter_max(self._c_counter)

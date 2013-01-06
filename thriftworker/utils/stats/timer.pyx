from libc.stdlib cimport malloc, free


cdef extern from "stdint.h" nogil:

    ctypedef unsigned int uint32_t
    ctypedef unsigned int uint64_t


cdef extern from "cm_quantile.h":

    ctypedef struct cm_quantile:
        pass


cdef extern from "cm_timer.h":

    ctypedef struct timer:
        uint64_t count
        double sum
        double squared_sum
        int finalized
        cm_quantile cm

    int init_timer(double eps, double *quantiles, uint32_t num_quants, timer *c_timer)
    int destroy_timer(timer *c_timer)

    int timer_add_sample(timer *c_timer, double sample)
    double timer_query(timer *c_timer, double quantile)
    uint64_t timer_count(timer *c_timer)
    double timer_min(timer *c_timer)
    double timer_mean(timer *c_timer)
    double timer_stddev(timer *c_timer)
    double timer_sum(timer *c_timer)
    double timer_squared_sum(timer *c_timer)
    double timer_max(timer *c_timer)


cdef class Timer(object):
    cdef timer *_c_timer
    cdef object quantiles

    def __cinit__(self, double eps=0.01, object quantile=None):
        # These are the quantiles we track
        self.quantiles = tuple(quantile or [0.5, 0.95, 0.99])
        assert self.quantiles, 'list of quantiles empty'
        cdef double *quantiles = []
        for i, quantile in enumerate(self.quantiles):
            quantiles[i] = quantile
        cdef int num_quantiles = len(self.quantiles)
        self._c_timer = <timer *>malloc(sizeof(timer))
        if self._c_timer is NULL:
            raise MemoryError("Can't create timer struct")
        assert init_timer(eps, quantiles, num_quantiles, self._c_timer) == 0

    def add(self, double sample):
        assert timer_add_sample(self._c_timer, sample) == 0

    def query(self, double quantile=0.95):
        assert quantile in self.quantiles, 'wrong quantile given'
        return timer_query(self._c_timer, quantile)

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
        return ('<{0}(count={2.count}, mean={2.mean}) at {1}>'.
                format(self.__class__.__name__, hex(id(self)), self))

    property mean:

        def __get__(self):
            return timer_mean(self._c_timer)

    property stddev:

        def __get__(self):
            return timer_stddev(self._c_timer)

    property sum:

        def __get__(self):
            return timer_sum(self._c_timer)

    property count:

        def __get__(self):
            return timer_count(self._c_timer)

    property squared_sum:

        def __get__(self):
            return timer_squared_sum(self._c_timer)

    property min:

        def __get__(self):
            return timer_min(self._c_timer)

    property max:

        def __get__(self):
            return timer_max(self._c_timer)

    def __dealloc__(self):
        if self._c_timer is not NULL:
            destroy_timer(self._c_timer)
            free(self._c_timer)

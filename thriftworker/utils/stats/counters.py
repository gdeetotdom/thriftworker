from __future__ import absolute_import

from collections import defaultdict

from .counter import Counter


class Counters(defaultdict):
    """Store counters here."""

    def __init__(self):
        super(Counters, self).__init__(Counter)

    def to_dict(self):
        """Convert all counters to dict."""
        return {key: {'mean': counter.mean,
                      'stddev': counter.stddev,
                      'sum': counter.sum,
                      'count': counter.count,
                      'squared_sum': counter.squared_sum,
                      'min': counter.min,
                      'max': counter.max}
                for key, counter in self.items()}

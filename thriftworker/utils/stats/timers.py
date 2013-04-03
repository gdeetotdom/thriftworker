from __future__ import absolute_import

from collections import defaultdict

from .timer import Timer


class Timers(defaultdict):

    def __init__(self):
        super(Timers, self).__init__(Timer)

    def to_dict(self):
        """Convert all timers to dict."""
        return {key: {'mean': timer.mean,
                      'stddev': timer.stddev,
                      'sum': timer.sum,
                      'count': timer.count,
                      'squared_sum': timer.squared_sum,
                      'min': timer.min,
                      'max': timer.max,
                      'distribution95': timer.query(0.95)}
                for key, timer in self.items()}

from __future__ import absolute_import

from collections import defaultdict

from .timer import Timer


class Timers(defaultdict):

    def __init__(self):
        super(Timers, self).__init__(Timer)

from __future__ import absolute_import

from collections import defaultdict

from .counter import Counter


class Counters(defaultdict):

    def __init__(self):
        super(Counters, self).__init__(Counter)

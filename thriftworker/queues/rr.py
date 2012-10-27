"""Round-robin queue."""
from __future__ import absolute_import

from collections import deque
from itertools import cycle

from ..utils.decorators import cached_property
from .base import BaseQueue


class RRQueue(BaseQueue):

    def __init__(self):
        self.streams = {}

    @cached_property
    def _cycle(self):
        return cycle(self.streams.keys())

    def register(self, identity):
        self.streams[identity] = deque()
        del self._cycle

    def push(self, value):
        identity = self._cycle.next()
        self.streams[identity].append(value)
        return identity

    def pop(self, identity):
        return self.streams[identity].popleft()

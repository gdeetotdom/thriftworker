# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
from collections import deque

import pyuv

logger = logging.getLogger(__name__)
noop = lambda h: None


class AsyncQueue(object):
    """Asynchronous queue used to receive messages.

    It allows you to queue messages that will be handled later by the
    application::

        # define the queue
        q = AsyncQueue(loop)

        # ... send a message
        q.send(callable)

    """

    def __init__(self, loop):
        self.loop = loop
        self._queue = deque()
        self._dispatcher = pyuv.Prepare(self.loop)
        self._dispatcher.start(self._send)
        if hasattr(self._dispatcher, 'unref'):
            self._dispatcher.unref()
        self._tick = pyuv.Async(loop, self._spin_up)
        self._spinner = pyuv.Idle(self.loop)

    def send(self, msg):
        """ add a message to the queue

        Send is the only thread-safe method of this queue. It means that any
        thread can send a message.

        """
        self._queue.append(msg)
        if not self._tick.closed:
            self._tick.send()

    def close(self):
        """ close the queue """
        self._queue.clear()
        if not self._dispatcher.closed:
            self._dispatcher.close()
        if not self._spinner.closed:
            self._spinner.close()
        if not self._tick.closed:
            self._tick.close()

    def _spin_up(self, handle):
        self._spinner.start(noop)

    def _send(self, handle):
        queue = self._queue
        while True:
            try:
                callback = queue.popleft()
            except IndexError:
                break
            else:
                callback()
        if self._spinner.active:
            self._spinner.stop()

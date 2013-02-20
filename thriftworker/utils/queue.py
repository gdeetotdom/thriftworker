# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
from collections import deque

import pyuv

logger = logging.getLogger(__name__)


class AsyncQueue(object):
    """Asynchronous queue used to receive messages.

    It allows you to queue messages that will be handled later by the
    application::

        # callback for the queue
        def cb(msg):
            # ... do something with the message
            print(msg)

        # define the queue
        q = AsyncQueue(loop, cb)

        # ... send a message
        q.send("some message")

    """

    def __init__(self, loop, callback):
        self.loop = loop
        self._queue = deque()
        self._dispatcher = pyuv.Prepare(self.loop)
        self._spinner = pyuv.Idle(self.loop)
        self._tick = pyuv.Async(loop, self._do_send)
        self._callback = callback

    def send(self, msg):
        """ add a message to the queue

        Send is the only thread-safe method of this queue. It means that any
        thread can send a message.

        """
        self._queue.append(msg)
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

    def _do_send(self, handle):
        if not self._dispatcher.active:
            self._dispatcher.start(self._send)
            self._spinner.start(lambda h: h.stop())

    def _send(self, handle):
        queue, self._queue = self._queue, deque()
        for msg in queue:
            try:
                self._callback(msg)
            except Exception as exc:
                logger.exception(exc)

        if not self._dispatcher.closed:
            self._dispatcher.stop()

"""A multi-producer, multi-consumer queue."""

from collections import deque
from time import time as _time

from pyuv import thread as _thread

__all__ = ['Empty', 'Full', 'Queue']


class Empty(Exception):
    """Exception raised by Queue.get(block=0)/get_nowait()."""
    pass


class Full(Exception):
    """Exception raised by Queue.put(block=0)/put_nowait()."""
    pass


class Queue(object):
    """Create a queue object with a given maximum size.
 
    If maxsize is <= 0, the queue size is infinite.
    """
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self.queue = deque()
        # mutex must be held whenever the queue is mutating.  All methods
        # that acquire mutex must release it before returning.  mutex
        # is shared between the three conditions, so acquiring and
        # releasing the conditions also acquires and releases mutex.
        self.mutex = _thread.Mutex()
        # Notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then.
        self.not_empty = _thread.Condition()
        # Notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then.
        self.not_full = _thread.Condition()
 
    def qsize(self):
        """Return the approximate size of the queue (not reliable!)."""
        self.mutex.lock()
        n = len(self.queue)
        self.mutex.unlock()
        return n
 
    def empty(self):
        """Return True if the queue is empty, False otherwise (not reliable!)."""
        self.mutex.lock()
        n = not len(self.queue)
        self.mutex.unlock()
        return n
 
    def full(self):
        """Return True if the queue is full, False otherwise (not reliable!)."""
        self.mutex.lock()
        n = 0 < self.maxsize == len(self.queue)
        self.mutex.unlock()
        return n
 
    def put(self, item, block=True, timeout=None):
        """Put an item into the queue.
 
        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        self.mutex.lock()
        try:
            if self.maxsize > 0:
                if not block:
                    if len(self.queue) == self.maxsize:
                        raise Full
                elif timeout is None:
                    while len(self.queue) == self.maxsize:
                        self.not_full.wait(self.mutex)
                elif timeout < 0:
                    raise ValueError("'timeout' must be a positive number")
                else:
                    endtime = _time() + timeout
                    while len(self.queue) == self.maxsize:
                        remaining = endtime - _time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.timedwait(self.mutex, remaining)
            self.queue.append(item)
            self.not_empty.signal()
        finally:
            self.mutex.unlock()
 
    def put_nowait(self, item):
        """Put an item into the queue without blocking.
 
        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        """
        return self.put(item, False)
 
    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.
 
        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        self.mutex.lock()
        try:
            if not block:
                if not len(self.queue):
                    raise Empty
            elif timeout is None:
                while not len(self.queue):
                    self.not_empty.wait(self.mutex)
            elif timeout < 0:
                raise ValueError("'timeout' must be a positive number")
            else:
                endtime = _time() + timeout
                while not len(self.queue):
                    remaining = endtime - _time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.timedwait(self.mutex, remaining)
            item = self.queue.popleft()
            self.not_full.signal()
            return item
        finally:
            self.mutex.unlock()
 
    def get_nowait(self):
        """Remove and return an item from the queue without blocking.
 
        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        """
        return self.get(False)

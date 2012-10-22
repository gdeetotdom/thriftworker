"""Provides thread safe pool."""
from __future__ import absolute_import

from Queue import LifoQueue, Empty

__all__ = ['LimitExceeded', 'ResourcePool']


class LimitExceeded(Exception):
    """Limit exceeded."""


class ResourcePool(object):
    """Resource holder. Create and store them."""

    def __init__(self):
        self._resource = LifoQueue()
        self._dirty = set()

    def add(self, resource):
        """Add new resource to pool."""
        self._resource.put_nowait(resource)

    def acquire(self, block=True, timeout=None):
        """Acquire resource.

        :keyword block: If the limit is exceeded,
          block until there is an available item.
        :keyword timeout: Timeout to wait
          if ``block`` is true. Default is :const:`None` (forever).

        :raises LimitExceeded: if block is false
          and the limit has been exceeded.

        """
        while 1:
            try:
                R = self._resource.get(block=block, timeout=timeout)
            except Empty:
                raise LimitExceeded()
            else:
                self._dirty.add(R)
                break

        def release():
            """Release resource so it can be used by another thread.

            The caller is responsible for discarding the object,
            and to never use the resource again.  A new resource must
            be acquired if so needed.

            """
            self.release(R)

        R.release = release

        return R

    def release(self, resource):
        self._dirty.discard(resource)
        self._resource.put_nowait(resource)

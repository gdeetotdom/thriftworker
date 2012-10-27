"""All exceptions for this package."""
from __future__ import absolute_import


class AllocationError(Exception):
    """Raised if we can't bind to any address from pool."""


class BindError(Exception):
    """Error on socket binding."""


class ServiceAlreadyRegistered(Exception):
    """Given service already registered in storage."""

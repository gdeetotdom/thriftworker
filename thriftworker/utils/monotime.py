from __future__ import absolute_import

try:
    from ._monotime import monotonic
except ImportError:
    from time import time as monotonic

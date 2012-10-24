"""Some function to work with system threads."""
from __future__ import absolute_import

import imp

__all__ = ['spawn']

_realthread = None


def get_realthread():
    """Get the real Python thread module, regardless of any monkeypatching"""
    global _realthread
    if _realthread:
        return _realthread

    fp, pathname, description = imp.find_module('thread')
    try:
        _realthread = imp.load_module('realthread', fp, pathname, description)
        return _realthread
    finally:
        if fp:
            fp.close()


def spawn(func, *args, **kwargs):
    """Takes a function and spawns it as a daemon thread using the
    real OS thread regardless of monkey patching.

    """
    return get_realthread().start_new_thread(func, args, kwargs)

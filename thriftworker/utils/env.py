"""Detect current environment."""
from __future__ import absolute_import

import sys

from thriftworker.constants import DEFAULT_ENV, GEVENT_ENV

_environment = None


def _detect_environment():

    # -gevent-
    if 'gevent' in sys.modules:
        try:
            from gevent import socket as _gsocket
            import socket

            if socket.socket is _gsocket.socket:
                return GEVENT_ENV
        except ImportError:
            pass

    return DEFAULT_ENV


def detect_environment():
    global _environment
    if _environment is None:
        _environment = _detect_environment()
    return _environment

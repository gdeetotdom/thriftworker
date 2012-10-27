"""Handle global package state."""
from __future__ import absolute_import

from .utils.proxy import Proxy

default_app = None


def set_current_app(app):
    global default_app
    default_app = app


def get_current_app():
    global default_app
    if default_app is None:
        # creates the default app, but we want to defer that.
        raise RuntimeError('No ThriftWorker was configured!')
    return default_app


current_app = Proxy(get_current_app)

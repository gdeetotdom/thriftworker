import os

if os.environ.get('USE_GEVENT', 0):
    from gevent.monkey import patch_all
    patch_all()
    del patch_all

del os

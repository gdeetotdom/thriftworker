from __future__ import absolute_import

from time import time, sleep


def wait(predicate, timeout=5.0, resolution=0.05):
    """Wait while predicate will return True or timeout exceeded."""
    tic = time() + timeout
    while not predicate() and tic > time():
        sleep(resolution)
    return predicate()

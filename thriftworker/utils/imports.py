from __future__ import absolute_import

import os
import sys
import imp
import importlib
from contextlib import contextmanager

import six


def symbol_by_name(name, aliases={}, imp=None, package=None,
                   sep='.', default=None, **kwargs):
    """Get symbol by qualified name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        celery.concurrency.processes.TaskPool
                                    ^- class name

    or using ':' to separate module and symbol::

        celery.concurrency.processes:TaskPool

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    Examples:

        >>> symbol_by_name("celery.concurrency.processes.TaskPool")
        <class 'celery.concurrency.processes.TaskPool'>

        >>> symbol_by_name("default", {
        ...     "default": "celery.concurrency.processes.TaskPool"})
        <class 'celery.concurrency.processes.TaskPool'>

        # Does not try to look up non-string names.
        >>> from celery.concurrency.processes import TaskPool
        >>> symbol_by_name(TaskPool) is TaskPool
        True

    """
    if imp is None:
        imp = importlib.import_module

    if not isinstance(name, basestring):
        return name                                 # already a class

    name = aliases.get(name) or name
    sep = ':' if ':' in name else sep
    module_name, _, cls_name = name.rpartition(sep)
    if not module_name:
        cls_name, module_name = None, package if package else cls_name
    try:
        try:
            module = imp(module_name, package=package, **kwargs)
        except ValueError, exc:
            exc = ValueError("Couldn't import %r: %s" % (name, exc))
            six.reraise(ValueError, exc, sys.exc_info()[2])
        return getattr(module, cls_name) if cls_name else module
    except (ImportError, AttributeError):
        if default is None:
            raise
    return default


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See :func:`symbol_by_name`.

    """
    return symbol_by_name(name)(*args, **kwargs)


def qualname(obj):
    if isinstance(obj, basestring):
        return obj
    if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
        return qualname(obj.__class__)
    return '.'.join([obj.__module__, obj.__name__])


def get_real_module(name):
    """Get the real Python module, regardless of any monkeypatching"""
    fp, pathname, description = imp.find_module(name)
    imp.acquire_lock()
    try:
        _realmodule = imp.load_module('_real_{0}'.format(name), fp, pathname,
                                      description)
        return _realmodule
    finally:
        imp.release_lock()
        if fp:
            fp.close()


@contextmanager
def cwd_in_path():
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:  # pragma: no cover
                pass


def import_from_cwd(module, imp=None, package=None):
    """Import module, but make sure it finds modules
    located in the current directory.

    Modules located in the current directory has
    precedence over modules located in `sys.path`.
    """
    if imp is None:
        imp = importlib.import_module
    with cwd_in_path():
        return imp(module, package=package)

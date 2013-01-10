"""Some useful mixins here."""
from __future__ import absolute_import

from ..state import current_app
from .imports import symbol_by_name
from .other import rgetattr


class LoopMixin(object):
    """Move loop closer to instance."""

    app = None

    @property
    def loop(self):
        """Shortcut to loop."""
        return self.app.loop


class StartStopMixin(object):
    """Implement some stubs method for object that can be started / stopped."""

    def start(self):
        pass

    def stop(self):
        pass


def _unpickle_appattr(name, args):
    """Given an attribute name and a list of args, gets
    the attribute from the current app and calls it.

    """
    return rgetattr(current_app, name)(*args)


class SubclassMixin(object):

    def subclass_with_self(self, Class, name=None, attribute='app',
                           reverse=None, **kw):
        """Subclass an app-compatible class by setting its app attribute
        to be this app instance.

        App-compatible means that the class has a class attribute that
        provides the default app it should use, e.g.
        ``class Foo: app = None``.

        :param Class: The app-compatible class to subclass.
        :keyword name: Custom name for the target class.
        :keyword attribute: Name of the attribute holding the app,
                            default is 'app'.

        """
        Class = symbol_by_name(Class)
        reverse = reverse if reverse else Class.__name__

        def __reduce__(self):
            return (_unpickle_appattr,
                    (reverse, self.__reduce_args__()))

        attrs = dict({attribute: self},
                     __module__=Class.__module__,
                     __reduce__=__reduce__,
                     __doc__=Class.__doc__, **kw)

        return type(name or Class.__name__, (Class,), attrs)


def _unpickle_appprop(name):
    """Given an attribute name, gets the attribute from the current app."""
    return rgetattr(current_app, name)


class PropertyMixin(object):

    def __reduce__(self):
        return (_unpickle_appprop, (self.__class__.__name__.lower(),))

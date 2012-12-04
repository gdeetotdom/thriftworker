from __future__ import absolute_import

import socket

from thriftworker.tests.utils import TestCase, StartStopLoopMixin, \
    start_stop_ctx
from thriftworker.listener import Listener, Listeners
from thriftworker.exceptions import BindError


class ListenerMixin(StartStopLoopMixin):

    Listener = None

    def setUp(self):
        super(ListenerMixin, self).setUp()
        self.Listener = self.app.subclass_with_self(type(self).Listener)


class TestListener(ListenerMixin, TestCase):

    Listener = Listener

    def test_start_stop(self):
        listener = self.Listener('SomeService', ('localhost', 0))
        with start_stop_ctx(listener):
            self.assertLess(0, listener.port)
            self.assertEqual(socket.gethostbyname('localhost'), listener.host)
            self.assertFalse(listener.channel.closed)

    def test_bind_error(self):
        first_listener = self.Listener('SomeService', ('localhost', 59357))
        second_listener = self.Listener('SomeService', ('localhost', 59357))
        with self.assertRaises(BindError):
            with start_stop_ctx(first_listener):
                with start_stop_ctx(second_listener):
                    pass

    def test_bind_from_pool(self):
        self.app.port_range = (59000, 59100)
        first_listener = self.Listener('SomeService', ('localhost', None))
        second_listener = self.Listener('SomeService', ('localhost', None))
        with start_stop_ctx(first_listener):
            with start_stop_ctx(second_listener):
                self.assertEqual(1, second_listener.port - first_listener.port)


class ListenersMixin(StartStopLoopMixin):

    Listeners = None

    def setUp(self):
        super(ListenersMixin, self).setUp()
        self.Listeners = self.app.subclass_with_self(type(self).Listeners)


class TestListeners(ListenersMixin, TestCase):

    Listeners = Listeners

    def test_register(self):
        listeners = self.Listeners()
        # Add first service.
        listeners.register('SomeService', 'localhost', None)
        registered = list(listeners)
        self.assertEqual(1, len(registered))
        listener = listeners[0]
        self.assertIs(listener, registered[0])
        self.assertIn(listener, listeners)
        self.assertEqual('SomeService', listener.name)
        self.assertEqual([listener.channel], listeners.channels)
        self.assertEqual({0: listener}, listeners.enumerated)
        # Add new service.
        listeners.register('OtherService', 'localhost', None)
        self.assertEqual([listeners[0].channel,
                          listeners[1].channel], listeners.channels)
        self.assertEqual({0: listeners[0],
                          1: listeners[1]}, listeners.enumerated)

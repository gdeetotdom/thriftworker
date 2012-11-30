from __future__ import absolute_import

from mock import Mock
from pyuv import Async

from thriftworker.tests.utils import TestCase, CustomAppMixin, \
    StartStopLoopMixin
from thriftworker.utils.loop import in_loop


class TestInLoop(StartStopLoopMixin, TestCase):

    def test_outside_loop(self):
        mock = Mock(return_value=None)

        class Entity(object):

            @in_loop
            def some_method(self, arg):
                return mock(arg)

        Entity().some_method(None)
        mock.assert_called_once_with(None)

    def test_inside_loop(self):
        mock = Mock(return_value=None)

        class Entity(object):

            @in_loop
            def some_method(self, async):
                return mock(async)

        def cb(handle):
            Entity().some_method(handle)
            handle.close()

        async = Async(self.loop, cb)
        async.send()

        self.wakeup_loop()
        mock.assert_called_once_with(async)

    def test_exception(self):

        class CustomException(Exception):
            pass

        class Entity(object):

            @in_loop
            def some_method(self):
                raise CustomException()

        with self.assertRaises(CustomException):
            Entity().some_method()


class TestOutsideLoop(CustomAppMixin, TestCase):

    def test_not_started_loop(self):

        class Entity(object):

            @in_loop
            def some_method(self):
                pass

        with self.assertRaises(RuntimeError):
            Entity().some_method()

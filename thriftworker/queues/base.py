"""Abstract queue."""
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod


class BaseQueue(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def register(self, identity):
        raise NotImplementedError()

    @abstractmethod
    def push(self, value):
        raise NotImplementedError()

    @abstractmethod
    def pop(self, identity):
        raise NotImplementedError()

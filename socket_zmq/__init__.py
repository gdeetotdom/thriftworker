"""Proxy from thrift framed transport to zmq."""

VERSION = (0, 1, 6)

__version__ = '.'.join(map(str, VERSION[0:3]))
__author__ = 'Lipin Dmitriy'
__contact__ = 'blackwithwhite666@gmail.com'
__homepage__ = 'https://github.com/blackwithwhite666/socket_zmq'
__docformat__ = 'restructuredtext'

# -eof meta-

from .app import SocketZMQ

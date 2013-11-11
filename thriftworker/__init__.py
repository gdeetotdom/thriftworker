"""Process incoming request and return it's result."""

VERSION = (0, 2, 9)

__version__ = '.'.join(map(str, VERSION[0:3]))
__author__ = 'Lipin Dmitriy'
__contact__ = 'blackwithwhite666@gmail.com'
__homepage__ = 'https://github.com/blackwithwhite666/thriftworker'
__docformat__ = 'restructuredtext'

# -eof meta-

from .app import ThriftWorker

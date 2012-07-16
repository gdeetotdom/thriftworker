from __future__ import with_statement
from setuptools import Extension, setup, find_packages
import logging
import os
import sys


# create default logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))


def fatal(msg, code=1):
    logger.error("Fatal: " + msg)
    exit(code)


if sys.version_info < (2, 7):
    fatal("socket_zmq requires Python 2.7.")


cmdclass = {}
extensions = []
extension_kwargs = {}


# try to find cython
try:
    from Cython.Distutils import build_ext
except ImportError:
    fatal("socket_zmq requires cython.")
else:
    cmdclass['build_ext'] = build_ext


# try to find zeromq
try:
    import zmq
except ImportError:
    fatal("socket_zmq requires pyzmq.")
else:
    extension_kwargs['include_dirs'] = zmq.get_includes()


def pyx(name):
    return os.path.abspath(os.path.join('socket_zmq', name + '.pyx'))


# collect extensions
for module in ['base', 'sink', 'source', 'pool', 'proxy']:
    ext = Extension('socket_zmq.{0}'.format(module),
                    sources=[pyx(module)],
                    **extension_kwargs)
    extensions.append(ext)


setup(
    name='socket_zmq',
    cmdclass=cmdclass,
    ext_modules=extensions,
    packages=find_packages(),
    install_requires=['pyzmq>=2.2.0,<3.0',
                      'Cython>=0.16',
                      'pyev>=0.8.1-4.04',
                      'thrift>=0.8.0'],
    classifiers=["Development Status :: 4 - Beta",
                 "Intended Audience :: Developers",
                 "Intended Audience :: System Administrators",
                 "License :: OSI Approved :: BSD License",
                 "License :: OSI Approved :: GNU General Public License (GPL)",
                 "Operating System :: POSIX",
                 "Programming Language :: Python :: 2.7"],
)

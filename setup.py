from setuptools import Extension, setup, find_packages
import os
import sys


if sys.version_info < (2, 7):
    raise Exception('ThriftPool requires Python 2.7.')


cmdclass = {}
extensions = []
extension_kwargs = {}


# try to find cython
try:
    from Cython.Distutils import build_ext
    cython_installed = True
except ImportError:
    cython_installed = False
else:
    cmdclass['build_ext'] = build_ext


# try to find zeromq
try:
    import zmq
    zmq_installed = True
except ImportError:
    zmq_installed = False
else:
    extension_kwargs['include_dirs'] = zmq.get_includes()


def source_extension(name):
    extension = '.pyx' if cython_installed else '.c'
    return os.path.join('socket_zmq', name + extension)


# collect extensions
for module in ['base', 'sink', 'source', 'pool', 'proxy']:
    ext = Extension('socket_zmq.{0}'.format(module),
                    sources=[source_extension(module)],
                    **extension_kwargs)
    extensions.append(ext)

package_data = {'socket_zmq': ['*.pxd']}


setup(
    name='socket_zmq',
    cmdclass=cmdclass,
    ext_modules=extensions,
    packages=find_packages(),
    package_data=package_data,
    install_requires=['pyzmq', 'Cython', 'pyev', 'thrift'],
    classifiers=["Development Status :: 4 - Beta",
                 "Intended Audience :: Developers",
                 "Intended Audience :: System Administrators",
                 "License :: OSI Approved :: BSD License",
                 "Operating System :: POSIX",
                 "Programming Language :: Python :: 2.7"],
)

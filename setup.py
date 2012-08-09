from distutils.core import setup
from distutils.extension import Extension
import os
import re
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

# Description, version and other meta information.

re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_vers = re.compile(r'VERSION\s*=\s*\((.*?)\)')
re_doc = re.compile(r'^"""(.+?)"""')
rq = lambda s: s.strip("\"'")


def add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, rq(attr_value)),)


def add_version(m):
    v = list(map(rq, m.groups()[0].split(', ')))
    return (('VERSION', '.'.join(v[0:3]) + ''.join(v[3:])),)


def add_doc(m):
    return (('doc', m.groups()[0]),)

pats = {re_meta: add_default,
        re_vers: add_version,
        re_doc: add_doc}
here = os.path.abspath(os.path.dirname(__file__))
meta_fh = open(os.path.join(here, 'socket_zmq/__init__.py'))
try:
    meta = {}
    for line in meta_fh:
        if line.strip() == '# -eof meta-':
            break
        for pattern, handler in pats.items():
            m = pattern.match(line.strip())
            if m:
                meta.update(handler(m))
finally:
    meta_fh.close()


setup(
    name='socket_zmq',
    version=meta['VERSION'],
    description=meta['doc'],
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    license='BSD',
    cmdclass=cmdclass,
    ext_modules=extensions,
    packages=['socket_zmq'],
    package_data=package_data,
    requires=['pyzmq', 'Cython', 'pyev', 'thrift'],
    classifiers=["Development Status :: 4 - Beta",
                 "Intended Audience :: Developers",
                 "Intended Audience :: System Administrators",
                 "License :: OSI Approved :: BSD License",
                 "Operating System :: POSIX",
                 "Programming Language :: Python :: 2.7"],
)

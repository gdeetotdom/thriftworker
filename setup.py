import os
import re
import sys

from distutils.core import setup
from distutils.extension import Extension
from distutils.command.sdist import sdist
from distutils.command.build_ext import build_ext


if sys.version_info < (2, 7):
    raise Exception('ThriftPool requires Python 2.7.')


#-----------------------------------------------------------------------------
# Flags and default values.
#-----------------------------------------------------------------------------

cmdclass = {}
extensions = []
extension_kwargs = {}


# try to find cython
try:
    from Cython.Distutils import build_ext as build_ext_c
    cython_installed = True
except ImportError:
    cython_installed = False


#-----------------------------------------------------------------------------
# Commands
#-----------------------------------------------------------------------------

class CheckSDist(sdist):
    """Custom sdist that ensures Cython has compiled all pyx files to c."""

    def initialize_options(self):
        sdist.initialize_options(self)
        self._pyxfiles = []
        for root, dirs, files in os.walk('socket_zmq'):
            for f in files:
                if f.endswith('.pyx'):
                    self._pyxfiles.append(os.path.join(root, f))

    def run(self):
        if 'cython' in cmdclass:
            self.run_command('cython')
        else:
            for pyxfile in self._pyxfiles:
                cfile = pyxfile[:-3] + 'c'
                msg = "C-source file '%s' not found."%(cfile)+\
                " Run 'setup.py cython' before sdist."
                assert os.path.isfile(cfile), msg
        sdist.run(self)

cmdclass['sdist'] = CheckSDist


if cython_installed:

    class CythonCommand(build_ext_c):
        """Custom distutils command subclassed from Cython.Distutils.build_ext
        to compile pyx->c, and stop there. All this does is override the
        C-compile method build_extension() with a no-op."""

        description = "Compile Cython sources to C"

        def build_extension(self, ext):
            pass

    class zbuild_ext(build_ext_c):
        def run(self):
            return build_ext.run(self)

    cmdclass['cython'] = CythonCommand
    cmdclass['build_ext'] = zbuild_ext

else:

    class CheckingBuildExt(build_ext):
        """Subclass build_ext to get clearer report if Cython is neccessary."""

        def check_cython_extensions(self, extensions):
            for ext in extensions:
                for src in ext.sources:
                    msg = "Cython-generated file '%s' not found." % src
                    assert os.path.exists(src), msg

        def build_extensions(self):
            self.check_cython_extensions(self.extensions)
            self.check_extensions_list(self.extensions)

            for ext in self.extensions:
                self.build_extension(ext)

    cmdclass['build_ext'] = CheckingBuildExt


#-----------------------------------------------------------------------------
# Extensions
#-----------------------------------------------------------------------------


suffix = '.pyx' if cython_installed else '.c'


def source_extension(name):
    return os.path.join('socket_zmq', name + suffix)


# collect extensions
for module in ['base', 'sink', 'source', 'pool', 'proxy']:
    sources = [source_extension(module)]
    ext = Extension('socket_zmq.{0}'.format(module),
                    sources=sources,
                    **extension_kwargs)
    if suffix == '.pyx' and ext.sources[0].endswith('.c'):
        # undo setuptools stupidly clobbering cython sources:
        ext.sources = sources
    extensions.append(ext)

extensions.append(Extension('socket_zmq.vector_io',
                            sources=[os.path.join('src', 'vector_io.c')]))


#-----------------------------------------------------------------------------
# Description, version and other meta information.
#-----------------------------------------------------------------------------

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


#-----------------------------------------------------------------------------
# Setup
#-----------------------------------------------------------------------------

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
    requires=['pyev_static (>=0.9.0)', 'pyzmq (>=2.2.0)', 'thrift (>=0.8.0)'],
    classifiers=["Development Status :: 4 - Beta",
                 "Intended Audience :: Developers",
                 "Intended Audience :: System Administrators",
                 "License :: OSI Approved :: BSD License",
                 "Operating System :: POSIX",
                 "Programming Language :: Python :: 2.7"],
)

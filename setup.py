import os
import re
import sys

from setuptools import setup, Extension, find_packages
from setuptools.command.sdist import sdist
from setuptools.command.build_ext import build_ext


if sys.version_info < (2, 7):
    raise Exception('ThriftWorker requires Python 2.7.')


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


# current location
here = os.path.abspath(os.path.dirname(__file__))


#-----------------------------------------------------------------------------
# Commands
#-----------------------------------------------------------------------------

class CheckSDist(sdist):
    """Custom sdist that ensures Cython has compiled all pyx files to c."""

    def initialize_options(self):
        sdist.initialize_options(self)
        self._pyxfiles = []
        for root, dirs, files in os.walk('thriftworker'):
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
            from distutils.command.build_ext import build_ext as _build_ext
            return _build_ext.run(self)

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
    parts = name.split('.')
    parts[-1] = parts[-1] + suffix
    return os.path.join('thriftworker', *parts)


def prepare_sources(sources):
    def to_string(s):
        if isinstance(s, unicode):
            return s.encode('utf-8')
        return s
    return [to_string(source) for source in sources]


modules = {
    'transports.framed.connection': dict(),
    'transports.utils': dict(),
    'utils._monotime': dict(
        sources=[
            os.path.join(here, 'src', 'monotime.c'),
        ],
        extra_link_args=['-lrt'] if sys.platform != 'darwin' else [],
    ),
    'utils.stats.counter': dict(
        include_dirs=[os.path.join(here, 'src')],
        sources=[
            source_extension('utils.stats.counter'),
            os.path.join(here, 'src', 'cm_counter.c'),
        ],
        extra_compile_args=['-std=c99'],
    ),
    'utils.stats.timer': dict(
        include_dirs=[os.path.join(here, 'src')],
        sources=[
            source_extension('utils.stats.timer'),
            os.path.join(here, 'src', 'cm_timer.c'),
            os.path.join(here, 'src', 'cm_quantile.c'),
            os.path.join(here, 'src', 'cm_heap.c'),
        ],
        extra_compile_args=['-std=c99'],
    ),
    'utils.atomics.integer': dict(
        extra_compile_args=['-std=c99'],
    ),
    'utils.atomics.boolean': dict(
        extra_compile_args=['-std=c99'],
    ),
}

# collect extensions
for module, kwargs in modules.items():
    kwargs = dict(extension_kwargs, **kwargs)
    kwargs.setdefault('sources', [source_extension(module)])
    kwargs['sources'] = prepare_sources(kwargs['sources'])
    ext = Extension('thriftworker.{0}'.format(module), **kwargs)
    if suffix == '.pyx' and ext.sources[0].endswith('.c'):
        # undo setuptools stupidly clobbering cython sources:
        ext.sources = kwargs['sources']
    extensions.append(ext)


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
meta_fh = open(os.path.join(here, 'thriftworker/__init__.py'))
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

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(here, 'CHANGES.rst')) as f:
    CHANGES = f.read()


#-----------------------------------------------------------------------------
# Setup
#-----------------------------------------------------------------------------

setup(
    name='thriftworker',
    version=meta['VERSION'],
    description=meta['doc'],
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    long_description=README + '\n\n' + CHANGES,
    keywords='thrift soa',
    license='BSD',
    cmdclass=cmdclass,
    ext_modules=extensions,
    packages=find_packages(),
    install_requires=[
        'pyuv>=0.10.0',
        'thrift>=0.8.0',
        'six>=1.2.0',
        'greenlet>=0.4.0',
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
    ],
)

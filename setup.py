from setuptools import setup
from setuptools.command.test import test as TestCommand


class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        import tox
        errno = tox.cmdline(self.test_args)
        exit(errno)


with open('README.rst') as reader:
    readme = reader.read()

__title__ = 'diskcache'
__version__ = '3.1.1'
__build__ = 0x030101
__author__ = 'Grant Jenks'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016-2018 Grant Jenks'


setup(
    name=__title__,
    version=__version__,
    description='Disk Cache -- Disk and file backed persistent cache.',
    long_description=readme,
    author='Grant Jenks',
    author_email='contact@grantjenks.com',
    url='http://www.grantjenks.com/docs/diskcache/',
    license='Apache 2.0',
    packages=['diskcache'],
    tests_require=['tox'],
    cmdclass={'test': Tox},
    install_requires=["aiofiles", "aiosqlite"],
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ),
)

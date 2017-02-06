name = 'newt.db'
version = '0.2.1'

install_requires = ['setuptools', 'RelStorage[postgresql]', 'six']
extras_require = dict(test=['manuel', 'mock', 'zope.testing', 'zc.zlibstorage'])

entry_points = """
"""

from setuptools import setup

long_description = open('README.rst').read() + '\n' + open('CHANGES.rst').read()

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: MIT License
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6
Programming Language :: Python :: Implementation :: CPython
Programming Language :: Python :: Implementation :: PyPy
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
Framework :: ZODB
""".strip().split('\n')

setup(
    author = 'Jim Fulton',
    author_email = 'jim@jimfulton.info',
    license = 'MIT',
    url = 'http://www.newtdb.org/',

    name = name,
    version = version,
    long_description = long_description,
    description = long_description.strip().split('\n')[1],
    packages = [name.split('.')[0], name],
    namespace_packages = [name.split('.')[0]],
    package_dir = {'': 'src'},
    install_requires = install_requires,
    keywords="database nosql python postgresql postgres",
    zip_safe = False,
    entry_points=entry_points,
    package_data = {name: ['*.txt', '*.test', '*.html']},
    extras_require = extras_require,
    tests_require = extras_require['test'],
    classifiers = classifiers,
    )

#!/usr/bin/env python

import codecs
import os
import re

from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession


NAME = 'eavesdroppr'
VERSION = '0.9.1'
PACKAGES = find_packages(where='src')
DEPENDENCIES=['docopt',
              'docutils',
              'psycopg2',
              'Jinja2',
              'MarkupSafe',
              'PyYAML',
              'SQLAlchemy',
              'snap-micro',
              'teamcity-messages']

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='eavesdroppr',
    version=VERSION,
    author='Dexter Taylor',
    author_email='binarymachineshop@gmail.com',
    platforms=['any'],
    scripts=['scripts/eavesdrop'],
    packages=find_packages(),
    install_requires=DEPENDENCIES,
    test_suite='tests',
    description=('Eavesdroppr: Eavesdrop on Postgres Records'),
    license='MIT',
    keywords='postgres database events listen notify',
    url='http://github.com/binarymachines/eavesdroppr',
    long_description=read('README.txt'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Software Development'
    ]
)

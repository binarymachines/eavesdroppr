#!/usr/bin/env python

import codecs
import os
import re

from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession

NAME = 'eavesdroppr'
PACKAGES = find_packages(where='src')
DEPENDENCIES=['docopt',
              'docutils',
              'psycopg2',
              'Jinja2',
              'MarkupSafe',
              'PyYAML',
              'SQLAlchemy',
              'snap-micro',
              'pgpubsub',
              'teamcity-messages']


install_reqs = parse_requirements('requirements.txt', session=PipSession())
reqs = [str(ir.req) for ir in install_reqs]
eavesdroppr_version = os.getenv('EAVESDROPPR_VERSION')
if not eavesdroppr_version:
    raise Exception('The environment variable EAVESDROPPR_VERSION has not been set.')

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='eavesdroppr',
    version='%s' % eavesdroppr_version,
    author='Dexter Taylor',
    author_email='binarymachineshop@gmail.com',
    platforms=['any'],
    scripts=['scripts/eavesdrop'],
    packages=find_packages(),
    install_requires=DEPENDENCIES,                    
    test_suite='tests',
    description=('A framework for generating and handling listen/notify events from postgres'),
    license='MIT',
    keywords='postgres database events',
    url='http://github.com/binarymachines/eavesdroppr',
    long_description=read('README.txt'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Software Development'
    ]
)

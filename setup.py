#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(
    name='ecbdatahandler',
    version='1.0',
    description='Data handling tool for ECB',
    author='Celio Passos',
    author_email='celio.passosjr@gmail.com',
    packages=find_packages(where='.', exclude=('virtualenv')),
    scripts=['bin/ecbdatahandler']
)

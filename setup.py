#!/usr/bin/python
# -*- encoding: utf-8 -*-

from setuptools import setup, find_packages

version = '0.11'

setup(
    name='rabbit_rpc',
    version=version,
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'pika'
    ],
    include_package_data=True,
    package_dir={'rabbit_rpc': 'rabbit_rpc'}
)

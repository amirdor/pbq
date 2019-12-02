#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

requirements = ['google-api-core==1.14.*', 'google-cloud-bigquery-storage==0.7.*', 'google-cloud-bigquery==1.20.*',
                'google-cloud-core==1.0.*', 'pandas==0.25.*', 'pyarrow==0.14.*', 'fastparquet==0.3.*', 'requests== 2.22.*']

setup_requirements = requirements

test_requirements = requirements

setup(
    author="Dor Amir",
    author_email='amirdor@gmail.com',
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="python bigquery driver for easy access",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '<br><br>' + history,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='pbq',
    name='pbq',
    packages=find_packages(include=['pbq', 'pbq.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/amirdor/pbq',
    version='0.1.7',
    zip_safe=False,
)

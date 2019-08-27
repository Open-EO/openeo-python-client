import re

from setuptools import setup, find_packages
import os
import datetime

# Load the openeo version info.
#
# Note that we cannot simply import the module, since dependencies listed
# in setup() will very likely not be installed yet when setup.py run.
#
# See:
#   https://packaging.python.org/guides/single-sourcing-package-version

__version__ = None
date = datetime.datetime.today().strftime('%Y%m%d')

with open('openeo/_version.py') as fp:
    exec(fp.read())

if os.environ.get('BUILD_NUMBER') and os.environ.get('BRANCH_NAME'):
    if os.environ.get('BRANCH_NAME') == 'develop':
        version = __version__ + '.' + date + '.' + os.environ['BUILD_NUMBER']
    else:
        version = __version__ + '.' + date + '.' + os.environ['BUILD_NUMBER'] + '+' + os.environ['BRANCH_NAME']
else:
    version = __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

name = 'openeo'
setup(name=name,
      version=version,
      author='Jeroen Dries',
      author_email='jeroen.dries@vito.be',
      description='Client API for openEO',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/Open-EO/openeo-python-client",
      packages=find_packages(include=['openeo*']),
      setup_requires=['pytest-runner'],
      tests_require=['pytest','mock','requests-mock'],
      test_suite = 'tests',
      install_requires=[
          'requests',
          'shapely>=1.6.4',
          'cloudpickle',
          'numpy',
          'pandas',
          'deprecated',
      ],
      classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
      ]
      )

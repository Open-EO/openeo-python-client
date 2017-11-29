import re

from setuptools import setup

test_requirements = ['requests','mock']

with open('openeo/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

setup(name='openeo-api',
      version=version,
      author='Jeroen Dries',
      author_email='jeroen.dries@vito.be',
      description='Client API for OpenEO',
      packages=['openeo'],
      install_requires=['requests','shapely==1.5.17'])

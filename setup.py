import re

from setuptools import setup, find_packages

with open('openeo/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)
name = 'openeo-api'
setup(name=name,
      version=version,
      author='Jeroen Dries',
      author_email='jeroen.dries@vito.be',
      description='Client API for OpenEO',
      packages=find_packages(include=['openeo*']),
      test_requirements=['requests-mock'],
      install_requires=['requests', 'shapely>=1.6.4', 'cloudpickle', 'numpy', 'pandas'],
      )

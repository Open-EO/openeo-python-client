import re

from setuptools import setup
from sphinx.setup_command import BuildDoc

test_requirements = ['requests','mock']
cmdclass = {'build_sphinx': BuildDoc}

with open('openeo/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)
name = 'openeo-api'
setup(name=name,
      version=version,
      author='Jeroen Dries',
      author_email='jeroen.dries@vito.be',
      description='Client API for OpenEO',
      packages=['openeo'],
      install_requires=['requests','shapely==1.5.17'],
      cmdclass=cmdclass,
      # these are optional and override conf.py settings
      command_options={
          'build_sphinx': {
              'project': ('setup.py', name),
              'version': ('setup.py', version),
              'release': ('setup.py', 0)}},)

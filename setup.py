from setuptools import setup, find_packages

# Load the openeo version info.
#
# Note that we cannot simply import the module, since dependencies listed
# in setup() will very likely not be installed yet when setup.py run.
#
# See:
#   https://packaging.python.org/guides/single-sourcing-package-version


_version = {}
with open('openeo/_version.py') as fp:
    exec(fp.read(), _version)


with open("README.md", "r") as fh:
    long_description = fh.read()

name = 'openeo'
setup(name=name,
      version=_version['__version__'],
      author='Jeroen Dries',
      author_email='jeroen.dries@vito.be',
      description='Client API for openEO',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/Open-EO/openeo-python-client",
      packages=find_packages(include=['openeo*']),
      setup_requires=['pytest-runner'],
      tests_require=['pytest', 'mock', 'requests-mock'],
      test_suite='tests',
      install_requires=[
          'requests',
          'requests_mock',
          'shapely>=1.6.4',
          'numpy>=1.17.0',
          'pandas>0.20.0;python_version>="3.5.3"',
          'pandas<0.25.0;python_version<"3.5.3"',
          'deprecated',
      ],
      extras_require={
          "dev": [
              "sphinx",
              "sphinx-autodoc-annotation",
              "sphinx-autodoc-typehints",
              "mock",
              "requests-mock",
              "pytest",
              "flake8",
          ]
      },
      classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
      ]
      )

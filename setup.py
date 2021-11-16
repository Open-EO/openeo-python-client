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

tests_require = [
    'pytest>=4.5.0',
    'mock',
    'requests-mock>=1.8.0',
    'h5netcdf',
    'matplotlib',
    'geopandas;python_version>="3.6"',
]

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
      include_package_data=True,
      tests_require=tests_require,
      test_suite='tests',
      install_requires=[
          'requests>=2.26.0',
          'shapely>=1.6.4',
          'numpy>=1.17.0',
          'xarray>=0.12.3',
          'pandas>0.20.0;python_version>="3.5.3"',
          'pandas<0.25.0;python_version<"3.5.3"',
          'deprecated>=1.2.12'
      ],
      extras_require={
          "dev": tests_require + [
              "sphinx",
              "sphinx-autodoc-annotation",
              "sphinx-autodoc-typehints",
              "flake8",
          ]
      },
      entry_points={
          "console_scripts": ["openeo-auth=openeo.rest.auth.cli:main"],
      },
      classifiers=[
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Programming Language :: Python :: 3.8",
          "License :: OSI Approved :: Apache Software License",
          "Development Status :: 5 - Production/Stable",
          "Operating System :: OS Independent",
      ]
      )

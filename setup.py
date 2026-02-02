from setuptools import find_packages, setup

# Load the openeo version info.
#
# Note that we cannot simply import the module, since dependencies listed
# in setup() will very likely not be installed yet when setup.py run.
#
# See:
#   https://packaging.python.org/guides/single-sourcing-package-version


_version = {}
with open("openeo/_version.py") as fp:
    exec(fp.read(), _version)

with open("README.md", "r") as fh:
    long_description = fh.read()

tests_require = [
    "pytest>=4.5.0",
    "mock",
    "requests-mock>=1.8.0",
    "httpretty>=1.1.4",
    "urllib3<2.3.0",  # httpretty doesn't work properly with urllib3>=2.3.0. See #700 and https://github.com/gabrielfalcao/HTTPretty/issues/484
    "netCDF4>=1.7.0",
    # TODO #717 Simplify geopandas constraints when Python 3.8 support is dropped
    "geopandas>=0.14; python_version>='3.9'",
    "geopandas",  # Best-effort geopandas dependency for Python 3.8
    "flake8>=5.0.0",
    "time_machine>=2.13.0",
    "pyproj>=3.2.0",  # Pyproj is an optional, best-effort runtime dependency
    "dirty_equals>=0.8.0",
    "pyarrow>=10.0.1",  # For Parquet read/write support in pandas
    "python-dateutil>=2.7.0",
    "pystac-client>=0.7.5",
    "moto>=5.0.0",
]

docs_require = [
    "sphinx",
    "sphinx-autodoc-annotation",
    "sphinx-autodoc-typehints>=2.2.3",
    "myst-parser",
]

localprocessing_require = [
    "rioxarray>=0.13.0",
    "pyproj",
    "openeo_pg_parser_networkx>=2023.5.1",
    "openeo_processes_dask[implementations]>=2023.7.1",
]

jupyter_require = [
    "ipyleaflet>=0.17.0",
    "ipython",
]

artifacts_require = ["boto3", "botocore"]

typing_requires = ["types-boto3-s3", "types-boto3-sts"]

name = "openeo"
setup(
    name=name,
    version=_version["__version__"],
    author="Jeroen Dries",
    author_email="jeroen.dries@vito.be",
    description="Client API for openEO",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Open-EO/openeo-python-client",
    python_requires=">=3.8",
    packages=find_packages(include=["openeo*"]),
    include_package_data=True,
    tests_require=tests_require,
    test_suite="tests",
    install_requires=[
        "requests>=2.26.0",
        "urllib3>=1.9.0",
        "shapely>=1.6.4",
        "numpy>=1.17.0",
        "xarray>=0.12.3,<2025.01.2",  # TODO #721 xarray non-nanosecond support
        "pandas>0.20.0",
        # TODO #578: pystac 1.5.0 is highest version available for lowest Python version we still support (3.7).
        "pystac>=1.5.0",
        "deprecated>=1.2.12",
        'oschmod>=0.3.12; sys_platform == "win32"',
        "importlib_resources; python_version<'3.9'",
    ],
    extras_require={
        "tests": tests_require + artifacts_require,
        "dev": tests_require + docs_require + typing_requires + artifacts_require,
        "docs": docs_require,
        "oschmod": [  # install oschmod even when platform is not Windows, e.g. for testing in CI.
            "oschmod>=0.3.12"
        ],
        "localprocessing": localprocessing_require,
        "jupyter": jupyter_require,
        "artifacts": artifacts_require,
    },
    entry_points={
        "console_scripts": ["openeo-auth=openeo.rest.auth.cli:main"],
    },
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
    ],
    project_urls={
        "Documentation": "https://open-eo.github.io/openeo-python-client/",
        "Source Code": "https://github.com/Open-EO/openeo-python-client",
        "Bug Tracker": "https://github.com/Open-EO/openeo-python-client/issues",
        "Changelog": "https://github.com/Open-EO/openeo-python-client/blob/master/CHANGELOG.md",
    },
)

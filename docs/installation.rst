*************
Installation
*************


It is an explicit goal of the openEO Python client library to be as easy to install as possible,
unlocking the openEO ecosystem to a broad audience.
The package is a pure Python implementation and its dependencies are carefully considered (in number and complexity).


Basic install
=============

It is recommended to work in a some kind of *virtual environment* (``venv``, ``conda``, ...)
to avoid polluting the base install of Python on your operating system
or introducing conflicts with other applications.
How you organize your virtual environments heavily depends on your use case and workflow,
and is out of scope of this documentation.


Installation with ``pip``
-------------------------

The openEO Python client library is available from `PyPI <https://pypi.org/project/openeo/>`_
and can be easily installed with a tool like ``pip``, for example:

.. code-block:: console

    $ pip install openeo

To upgrade the package to the latest release:

.. code-block:: console

    $ pip install --upgrade openeo


Installation with Conda
------------------------

The openEO Python client library is available on `conda-forge <https://anaconda.org/conda-forge/openeo>`_
and can be easily installed in a conda environment, for example:

.. code-block:: console

    $ conda install -c conda-forge openeo


Verifying and troubleshooting
-----------------------------

You can check if the installation worked properly
by trying to import the ``openeo`` package in a Python script, interactive shell or notebook:

.. code-block:: python

    import openeo

    print(openeo.client_version())

This should print the installed version of the ``openeo`` package.

If the first line gives an error like ``ModuleNotFoundError: No module named 'openeo'``,
some troubleshooting tips:

-   Restart you Python shell or notebook (or start a fresh one).
-   Double check that the installation went well,
    e.g. try re-installing and keep an eye out for error/warning messages.
-   Make sure that you are working in the same (virtual) environment you installed the package in.

If you still have troubles installing and importing ``openeo``,
feel free to reach out in the `community forum <https://forums.openeo.cloud/>`_
or the `project's issue tracker <https://github.com/Open-EO/openeo-python-client/issues>`_.
Try to describe your setup in enough detail: your operating system,
which virtual environment system you use,
the installation tool (``pip``, ``conda`` or something else), ...



.. _installation-optional-dependencies:

Optional dependencies
======================

Depending on your use case, you might also want to install some additional libraries.
For example:

- ``netCDF4`` or ``h5netcdf`` for loading and writing NetCDF files (e.g. integrated in ``xarray.load_dataset()``)
- ``matplotlib`` for visualisation (e.g. integrated plot functionality in ``xarray`` )
- ``pyarrow`` for (read/write) support of Parquet files
  (e.g. with :py:class:`~openeo.extra.job_management.MultiBackendJobManager`)
- ``rioxarray`` for GeoTIFF support in the assert helpers from ``openeo.testing.results``
- ``geopandas`` for working with dataframes with geospatial support,
  (e.g. with :py:class:`~openeo.extra.job_management.MultiBackendJobManager`)


Enabling additional features
----------------------------

To use the on-demand preview feature and other Jupyter-enabled features, you need to install the necessary dependencies.

.. code-block:: console

    $ pip install openeo[jupyter]


Source or development install
==============================

If you closely track the development of the ``openeo`` package at
`github.com/Open-EO/openeo-python-client <https://github.com/Open-EO/openeo-python-client>`_
and want to work with unreleased features or contribute to the development of the package,
you can install it as follows from the root of a git source checkout:

.. code-block:: console

    $ pip install -e .[dev]

The ``-e`` option enables "development mode", which makes sure that changes you make to the source code
happen directly on the installed package, so that you don't have to re-install the package each time
you make a change.

The ``[dev]`` (a so-called "extra") installs additional development related dependencies,
for example to run the unit tests.

You can also find more information about installation for development on the :ref:`development-and-maintenance` page.

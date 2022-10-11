*************
Installation
*************


It is an explicit goal of the openEO Python client library to be as easy to install as possible,
unlocking the openEO ecosystem to a broad audience.
The package is a pure Python implementation and its dependencies are carefully considered (in number and complexity).


Basic install
=============

At least *Python 3.6* is recommended.
Also, it is recommended to work in a some kind of *virtual environment* (``venv``, ``conda``, ...)
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

This is work in progress. See `openeo-python-client#176 <https://github.com/Open-EO/openeo-python-client/issues/176>`_ for more information.

If you want to set up the openeo client for development on Windows with conda: see :ref:`windows-dev-install-with-conda`.

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


Optional dependencies
======================

Depending on your use case, you might also want to install some additional libraries.
For example:

- ``netCDF4`` or ``h5netcdf`` for loading and writing NetCDF files (e.g. integrated in ``xarray.load_dataset()``)
- ``matplotlib`` for visualisation (e.g. integrated plot functionality in ``xarray`` )




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


.. _windows-dev-install-with-conda:

Development Installation on Windows with ``conda``
--------------------------------------

There are some difficulties to install the development dependencies on Windows.
Namely, geopandas depends on a few libraries that are a bit trickier to install and for which there is no official python wheel.

The simplest way to do your development install for openeo is to use conda, either from Anaconda, or from Miniforge.

* `Anaconda <https://anaconda.org/>`_ is a commercial product, and `Miniforge <https://github.com/conda-forge/miniforge>` is a fully open source, drop-in replacement that gives you the same conda command.

* `Miniforge on GitHub <https://github.com/conda-forge/miniforge>`_

It is geopandas that depends on de more difficult libraries. One of is GDAL which written in C/C++.
So without python wheels or conda, you may need to install a C++ compiler and set it all up so pip can find it in your python environment or virtualenv.


The instructions below should work in both Anaconda and Miniforge.

Create a conda environment with the geopandas package already installed.
This is the step that avoids the hard part.

.. code-block:: console

    conda create -n <your environment's name>  geopandas

    # for example
    conda create -n openeopyclient  geopandas

Activate the conda environment

.. code-block:: console

    conda activate openeopyclient

Next, run the dev install with pip

In the directory where you git-cloned the openEO Python client:

.. code-block:: console

    python -m pip install -e .[dev]

Here is a quick way to check that the client was succesfully installed:
In your conda environment, launch the Python interpreter and try the following snippet of Python code to show the client's version:

.. code-block:: python

    import openeo

    print(openeo.client_version())



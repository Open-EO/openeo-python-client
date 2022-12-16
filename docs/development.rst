.. _development-and-maintenance:

###########################
Development and maintenance
###########################


For development on the ``openeo`` package itself,
it is recommended to install a local git checkout of the project
in development mode (``-e``)
with additional development related dependencies (``[dev]``)
like this::

    pip install -e .[dev]


Running the unit tests
======================

The test suite of the openEO Python Client leverages
the nice `pytest <https://docs.pytest.org/en/stable/>`_ framework.
It is installed automatically when installing the openEO Python Client
with the ``[dev]`` extra as shown above.
Running the whole tests is as simple as executing::

    pytest

There are a ton of command line options for fine-tuning
(e.g. select a subset of tests, how results should be reported, ...).
Run ``pytest -h`` for a quick overview
or check the `pytest <https://docs.pytest.org/en/stable/>`_ documentation for more information.

For example::

    # Skip tests that are marked as slow
    pytest -m "not slow"


Building the documentation
==========================

Building the documentation requires `Sphinx <https://www.sphinx-doc.org/en/master/>`_
and some plugins
(which are installed automatically as part of the ``[dev]`` install).

Quick and easy
---------------

The easiest way to build the documentation is working from the ``docs`` folder
and using the ``Makefile``:

.. code-block:: shell

    # From `docs` folder
    make html

(assumes you have ``make`` available, if not: use ``python -msphinx -M html .  _build``.)

This will generate the docs in HTML format under ``docs/_build/html/``.
Open the HTML files manually,
or use Python's built-in web server to host them locally, e.g.:

.. code-block:: shell

    # From `docs` folder
    python -m http.server 8000

Then, visit  http://127.0.0.1:8000/_build/html/ in your browser


Like a Pro
------------

When doing larger documentation work, it can be tedious to manually rebuild the docs
and refresh your browser to check the result.
Instead, use `sphinx-autobuild <https://github.com/executablebooks/sphinx-autobuild>`_
to automatically rebuild on documentation changes and live-reload it in your browser.
After installation (``pip install sphinx-autobuild`` in your development environment),
just run

.. code-block:: shell

    # From project root
    sphinx-autobuild docs/ --watch openeo/ docs/_build/html/

and then visit http://127.0.0.1:8000 .
When you change (and save) documentation source files, your browser should now
automatically refresh and show the newly build docs. Just like magic.


Creating a release
==================

This section describes the procedure to create
properly versioned releases of the ``openeo`` package
that can be downloaded by end users (e.g. through ``pip`` from pypi.org)
and depended on by other projects.

The releases will end up on:

- PyPi: `https://pypi.org/project/openeo <https://pypi.org/project/openeo/>`_
- VITO Artifactory: `https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple/openeo/ <https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple/openeo/>`_
- GitHub: `https://github.com/Open-EO/openeo-python-client/releases <https://github.com/Open-EO/openeo-python-client/releases>`_

Prerequisites
-------------

-   You have permissions to push branches and tags and maintain releases on
    the `openeo-python-client project on GitHub <https://github.com/Open-EO/openeo-python-client>`_.
-   You have permissions to upload releases to the
    `openeo project on pypi.org <https://pypi.org/project/openeo/>`_
-   The Python virtual environment you work in has the latest versions
    of the ``twine`` package installed.
    If you plan to build the wheel yourself (instead of letting Jenkins do this),
    you also need recent enough versions of the ``setuptools`` and ``wheel`` packages.

Important files
---------------

``setup.py``
    describes the metadata of the package,
    like package name ``openeo`` and version
    (which is extracted from ``openeo/_version.py``).

``openeo/_version.py``
    defines the version of the package.
    During general **development**, this version string should contain
    a `pre-release <https://www.python.org/dev/peps/pep-0440/#pre-releases>`_
    segment (e.g. ``a1`` for alpha releases, ``b1`` for beta releases, etc)
    to avoid collision with final releases. For example::

        __version__ = '0.4.7a1'

    As discussed below, this pre-release suffix should
    only be removed during the release procedure
    and restored when bumping the version after the release procedure.

``CHANGELOG.md``
    keeps track of important changes associated with each release.
    It follows the `Keep a Changelog <https://keepachangelog.com>`_ convention
    and should be properly updated with each bug fix, feature addition/removal, ...
    under the ``Unreleased`` section during development.

Procedure
---------

These are the steps to create and publish a new release of the ``openeo`` package.
To be as concrete as possible, we will assume that we are about to release version ``0.4.7``.

0.  Make sure you are working on **latest master branch**,
    without uncommitted changes and all tests are properly passing.

#.  Create release commit:

    A.  **Drop the pre-release suffix** from the version string in ``openeo/_version.py``
        so that it just a "final" semantic versioning string, e.g. ``0.4.7``

    B.  **Update CHANGELOG.md**: rename the "Unreleased" section title
        to contain version and date, e.g.::

            ## [0.4.7] - 2020-12-15

        remove empty subsections
        and start a new "Unreleased" section above it, like::

            ## [Unreleased]

            ### Added

            ### Changed

            ### Removed

            ### Fixed


    C.  **Commit** these changes in git with a commit message like ``Release 0.4.7``
        and **push** to GitHub::

            git add openeo/_version.py CHANGELOG.md
            git commit -m 'Release 0.4.7'
            git push origin master

#.  Optional, but recommended: wait for **VITO Jenkins** to build this updated master
    (trigger it manually if necessary),
    so that a build of a final, non-alpha release ``0.4.7``
    is properly uploaded to **VITO artifactory**.

#.  Create release on `PyPI <https://pypi.org/>`_:

    A.  **Obtain a wheel archive** of the package, with one of these approaches:

        -   *Preferably*: path of least surprise: build wheel through GitHub Actions.
            Go to workflow `"Build wheel" <https://github.com/Open-EO/openeo-python-client/actions/workflows/build-wheel.yml>`_,
            manually trigger a build with "Run workflow" button, wait for it to finish successfully,
            download generated ``artifact.zip``, and finally: unzip it to obtain ``openeo-0.4.7-py3-none-any.whl``

        -   *Or*, if you know what you are doing and you're sure you have a clean
            local checkout, you can also build it locally::

                python setup.py bdist_wheel

            This should create ``dist/openeo-0.4.7-py3-none-any.whl``

    B.  **Upload** this wheel to `PyPI <https://pypi.org/project/openeo/>`_::

            python -m twine upload openeo-0.4.7-py3-none-any.whl

        Check the `release history on PyPI <https://pypi.org/project/openeo/#history>`_
        to verify the twine upload.
        Another way to verify that the freshly created release installs
        is using docker to do a quick install-and-burn,
        for example as follows (check the installed version in pip's output)::

            docker run --rm -it python python -m pip install --no-deps openeo

#.  Create a **git version tag** and push it to GitHub::

        git tag v0.4.7
        git push origin v0.4.7

#.  Create a **release in GitHub**:
    Go to `https://github.com/Open-EO/openeo-python-client/releases/new <https://github.com/Open-EO/openeo-python-client/releases/new>`_,
    Enter ``v0.4.7`` under "tag",
    enter title: ``openEO Python Client v0.4.7``,
    use the corresponding ``CHANGELOG.md`` section as description
    and publish it
    (no need to attach binaries).

#.  **Bump version** in ``openeo/_version.py``,
    and append a pre-release "a1" suffix again, for example::

        __version__ = '0.4.8a1'

    Commit this (e.g. with message ``_version.py: next alpha version 0.4.8a1``)
    and push to GitHub.

#.  Optionally: send a tweet about the release or announce it in the `openEO Platform Forum <https://discuss.eodc.eu/c/openeo-platform/clients/18>`_ .


Verification
~~~~~~~~~~~~

The new release should now be available/listed at:

- `https://pypi.org/project/openeo/#history <https://pypi.org/project/openeo/#history>`_
- `https://github.com/Open-EO/openeo-python-client/releases <https://github.com/Open-EO/openeo-python-client/releases>`_

Here is a bash oneliner to verify that the PyPI release works properly::

    (cd /tmp &&\
        python -m venv tmp-venv-openeo &&\
        . tmp-venv-openeo/bin/activate &&\
        pip install openeo==0.4.7 &&\
        python -c "import openeo;print(openeo);print(openeo.__version__)"\
    )

It tries to install the package in a temporary virtual env,
import it and print the package version.


Development Installation on Windows
===================================

There can be a few difficulties to install the development dependencies on Windows via pip.

Namely, geopandas depends on a few libraries that are a bit trickier to install. They need some compiled code and unfortunately these libraries do not provide officially supported python wheels for Windows.

Cause: Down the line geopandas depends on GDAL and that is a C++ library that does not provide *official* binaries for Windows, though there are binaries from other sources.

Because there isn't supported binary or Python wheel, the pip installation process will try to compile the C libraries on the fly but that will only work if your have set up a C++ compiler properly.

Solutions
---------

These are a few solutions we know, ordered from the easiest option to the most complex one:

1. **Recommended option:** install the client in a conda environment, using either Anaconda or Miniforge. For most people this would be the simplest the solution: 

    See: :ref:`windows-dev-install-with-conda`

2. Use some unofficial python wheels for GDAL and Fiona. This is only suitable for development, not for production.

    See: :ref:`windows-dev-install-unofficial-wheels`

3. If you already use Docker or WSL, using either of those is also a good option for you.
4. Install a C++ compiler and deal with the compilation issues when you install it via pip.

.. _windows-dev-install-with-conda:

Option 1) Install the client in a conda environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The simplest way install your development setup for openeo is to use the conda package manager, either via Anaconda, or via Miniforge.

Anaconda is a commercial product and you can buy support for it. Miniforge is a fully open source alternative that has a drop-in replacement for the conda command.
Miniforge uses the `Conda-forge <https://conda-forge.org/>`_ channel (which is a package repository) by default.

* `Anaconda <https://anaconda.org/>`_
* `Miniforge on GitHub <https://github.com/conda-forge/miniforge>`_
* `Conda-forge <https://conda-forge.org/>`_

The instructions below should work in both Anaconda and Miniforge.
Though with Miniforge you can simplify the commands a little bit because the conda-forge channel is the default, so you can leave out the option ``-c conda-forge``.

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

A quick way to check whether the client was successfully installed or not is to print its version number.

In your conda environment, launch the Python interpreter and try the following snippet of Python code to show the client's version:

.. code-block:: python

    import openeo

    print(openeo.client_version())


.. _windows-dev-install-unofficial-wheels:

Option 2) Use some unofficial python wheels for GDAL and Fiona
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are `unofficial Python wheels at https://www.lfd.uci.edu/~gohlke/pythonlibs/: <https://www.lfd.uci.edu/~gohlke/pythonlibs/>`_

But as the name says, these wheels have no official support, so they are not recommended for production.
They can however help you out for a development environment.

You need to install the wheels for GDAL and Fiona.

* wheels for `Fiona <https://www.lfd.uci.edu/~gohlke/pythonlibs#fiona>`_
* wheels for `GDAL <https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal>`_

.. code-block::

    # In your activate virtualenv
    # install the wheels:
    pip install <path to GDAL whl file> <path to fiona whl file>

    # And then the regular developer installation command.
    python -m pip install -e .[dev]

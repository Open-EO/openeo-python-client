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

If you are on Windows and experience problems installing this way, you can find some solutions in section `Development Installation on Windows`_.

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
automatically refresh and show the newly built docs. Just like magic.


Contributing code
==================

User contributions (such as bug fixes and new features, both in source code and documentation)
are greatly appreciated and welcome.


Pull requests
--------------

We use a traditional `GitHub Pull Request (PR) <https://docs.github.com/en/pull-requests>`_ workflow
for user contributions, which roughly follows these steps:

- Create a personal fork of https://github.com/Open-EO/openeo-python-client
  (unless you already have push permissions to an existing fork or the original repo)
- Preferably: work on your contribution in a new feature branch
- Push your feature branch to your fork and create a pull request
- The pull request is the place for review, discussion and fine-tuning of your work
- Once your pull request is in good shape it will be merged by a maintainer


.. _precommit:

Pre-commit for basic code quality checks
------------------------------------------

We started using the `pre-commit <https://pre-commit.com/>`_ tool
for basic code quality fine-tuning of new contributions.
Note that the whole repository does not adhere yet to these new code styles rules at the moment,
we're just gradually introducing it, piggybacking on new contributions.

It's not enforced, but recommended and appreciated to **enable pre-commit**
in your git clone when contributing code, as follows:

-   Install the ``pre-commit`` command line tool:

    -   The simplest option is to install it directly in the *virtual environment*
        you are using for openEO Python client development (e.g. ``pip install pre-commit``).
    -   You can also install it *globally* on your system
        so you can easily use it across different projects
        (e.g. using `pipx <https://pypa.github.io/pipx/>`_, in your base conda environment, ...,
        see the `pre-commit installation docs <https://pre-commit.com/#installation>`_ for more information).
-   Install the git hook scripts in your local clone:

    .. code-block:: console

        pre-commit install

    This will automatically install additional tools required to check the rules
    defined in the ``.pre-commit-config.yaml`` configuration file.


Now, when you commit new changes, the pre-commit tool will run,
flag issues (e.g. invalid JSON files)
and even fix simple problems (e.g. clean up excessive whitespace).
Address these problems and try to commit again.

.. tip::

    Use the staging feature of git to prepare your commit.
    That makes it easier to review the changes proposed by pre-commit,
    as these are not staged automatically.


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

#.  Update `conda-forge package <https://github.com/conda-forge/openeo-feedstock>`_ too
    (requires conda recipe maintainer role).
    Normally, the "regro-cf-autotick-bot" will create a `pull request <https://github.com/conda-forge/openeo-feedstock/pulls>`_.
    If it builds fine, merge it.
    If not, fix the issue
    (typically in `recipe/meta.yaml <https://github.com/conda-forge/openeo-feedstock/blob/main/recipe/meta.yaml>`_)
    and merge.

#.  Optionally: send a tweet about the release
    or announce it in the `openEO Platform Forum <https://discuss.eodc.eu/c/openeo-platform/clients/18>`_.

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

Normally you can install the client the same way on Windows as on Linux, like so:

.. code-block:: console

    pip install -e .[dev]

This should be working with the most recent code, however, in the past we sometimes had issues with a development installation.

Should you experience problems, there is also a conda package for the Python client and that should be the easiest solution.

For development, what you need to do is:

1. Create and activate a new conda environment for developing the openeo-python-client.
2. In that conda environment, install only the dependencies of openeo via conda, but not the openeo package itself.
3. Do a pip install of the code in *editable mode* with `pip -e`.

.. code-block:: console

    conda create -n <your environment's name>

    # For example:
    conda create -n openeopyclient

    # Activate the conda environment
    conda activate openeopyclient

    # Install openeo from the conda-forge channel
    conda install --only-deps -c conda-forge openeo

    # Now install the openeo code in editable mode.
    pip install -e .[dev]



Update of generated files
==========================

Some parts of the openEO Python Client Library source code are
generated/compiled from upstream sources (e.g. official openEO specifications).
Because updates are not often required,
it's just a semi-manual procedure (to run from the project root):

.. code-block:: console

    # Update the sub-repositories (like git submodules, but optional)
    python specs/update-subrepos.py

    # Update `openeo/processes.py` from specifications in openeo-processes repository
    python openeo/internal/processes/generator.py  specs/openeo-processes specs/openeo-processes/proposals --output openeo/processes.py

    # Update the openEO process mapping documentation page
    python docs/process_mapping.py > docs/process_mapping.rst

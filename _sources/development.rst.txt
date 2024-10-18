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
for basic fine-tuning of code style and quality in new contributions.
It's currently not enforced, but **enabling pre-commit is recommended** and appreciated
when contributing code.

.. note::

    Note that the whole repository does not fully follow all code styles rules at the moment.
    We're just gradually introducing it, piggybacking on new contributions and commits.


Pre-commit set up
""""""""""""""""""

-   Install the general ``pre-commit`` command line tool:

    -   The simplest option is to install it directly in the *virtual environment*
        you are using for openEO Python client development (e.g. ``pip install pre-commit``).
    -   You can also install it *globally* on your system
        (e.g. using `pipx <https://pipx.pypa.io/>`_, conda, homebrew, ...)
        so you can use it across different projects.

-   Install the project specific git hook scripts by running this in the root of your local git clone:

    .. code-block:: console

        pre-commit install

    This will automatically install additional scripts and tools in a sandbox
    to run the various checks defined in the project's ``.pre-commit-config.yaml`` configuration file.

Pre-commit usage
"""""""""""""""""

When you commit new changes, the freshly installed pre-commit hook
will now automatically run each of the configured linters/formatters/...
Some of these just flag issues (e.g. invalid JSON files)
while others even automatically fix problems (e.g. clean up excessive whitespace).

If there is some kind of violation, the commit will be blocked.
Address these problems and try to commit again.

.. attention::

    Some pre-commit tools directly *edit* your files (e.g. formatting tweaks)
    instead of just flagging issues.
    This might feel intrusive at first, but once you get the hang of it,
    it should allow to streamline your workflow.

    In particular, it is recommended to use the *staging* feature of git to prepare your commit.
    Pre-commit's proposed changes are not staged automatically,
    so you can more easily keep them separate and review.

.. tip::

    You can temporarily disable pre-commit for these rare cases
    where you intentionally want to commit violating code style,
    e.g. through ``git commit`` command line option ``-n``/``--no-verify``.




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
    If you plan to build the wheel yourself (instead of letting GitHub or Jenkins do this),
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

        __version__ = '0.8.0a1'

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
To avoid the confusion with ad-hoc injection of some abstract version placeholder
that has to be replaced properly,
we will use a concrete version ``0.8.0`` in the examples below.

0.  Make sure you are working on **latest master branch**,
    without uncommitted changes and all tests are properly passing.

#.  Create release commit:

    A.  **Drop the pre-release suffix** from the version string in ``openeo/_version.py``
        so that it just a "final" semantic versioning string, e.g. ``0.8.0``

    B.  **Update CHANGELOG.md**: rename the "Unreleased" section title
        to contain version and date, e.g.::

            ## [0.8.0] - 2020-12-15

        remove empty subsections
        and start a new "Unreleased" section above it, like::

            ## [Unreleased]

            ### Added

            ### Changed

            ### Removed

            ### Fixed


    C.  **Commit** these changes in git with a commit message like ``Release 0.8.0``
        and **push** to GitHub::

            git add openeo/_version.py CHANGELOG.md
            git commit -m 'Release 0.8.0'
            git push origin master

#.  Optional, but recommended: wait for **VITO Jenkins** to build this updated master
    (trigger it manually if necessary),
    so that a build of a final, non-alpha release ``0.8.0``
    is properly uploaded to **VITO artifactory**.

#.  Create release on `PyPI <https://pypi.org/>`_:

    A.  **Obtain a wheel archive** of the package, with one of these approaches:

        -   *Preferably, the path of least surprise*: build wheel through GitHub Actions.
            Go to workflow `"Build wheel" <https://github.com/Open-EO/openeo-python-client/actions/workflows/build-wheel.yml>`_,
            manually trigger a build with "Run workflow" button, wait for it to finish successfully,
            download generated ``artifact.zip``, and finally: unzip it to obtain ``openeo-0.8.0-py3-none-any.whl``

        -   *Or, if you know what you are doing* and you're sure you have a clean
            local checkout, you can also build it locally::

                python setup.py bdist_wheel

            This should create ``dist/openeo-0.8.0-py3-none-any.whl``

    B.  **Upload** this wheel to `openeo project on PyPI <https://pypi.org/project/openeo/>`_::

            python -m twine upload openeo-0.8.0-py3-none-any.whl

        Check the `release history on PyPI <https://pypi.org/project/openeo/#history>`_
        to verify the twine upload.
        Another way to verify that the freshly created release installs
        is using docker to do a quick install-and-burn,
        for example as follows (check the installed version in pip's output)::

            docker run --rm -it python python -m pip install --no-deps openeo

#.  Create a **git version tag** and push it to GitHub::

        git tag v0.8.0
        git push origin v0.8.0

#.  Create a **release in GitHub**:
    Go to `https://github.com/Open-EO/openeo-python-client/releases/new <https://github.com/Open-EO/openeo-python-client/releases/new>`_,
    Enter ``v0.8.0`` under "tag",
    enter title: ``openEO Python Client v0.8.0``,
    use the corresponding ``CHANGELOG.md`` section as description
    and publish it
    (no need to attach binaries).

#.  **Bump the version** in ``openeo/_version.py``, (usually the "minor" level)
    and append a pre-release "a1" suffix again, for example::

        __version__ = '0.9.0a1'

    Commit this (e.g. with message ``_version.py: bump to 0.9.0a1``)
    and push to GitHub.

#.  Update `conda-forge package <https://github.com/conda-forge/openeo-feedstock>`_ too
    (requires conda recipe maintainer role).
    Normally, the "regro-cf-autotick-bot" will create a `pull request <https://github.com/conda-forge/openeo-feedstock/pulls>`_.
    If it builds fine, merge it.
    If not, fix the issue
    (typically in `recipe/meta.yaml <https://github.com/conda-forge/openeo-feedstock/blob/main/recipe/meta.yaml>`_)
    and merge.

#.  Optionally: make a post about the new release
    on the `openEO Platform Forum <https://discuss.eodc.eu/c/openeo-platform/clients/18>`_
    or the `CDSE Forum <https://forum.dataspace.copernicus.eu/c/openeo/28>`_.

Verification
"""""""""""""

The new release should now be available/listed at:

- `https://pypi.org/project/openeo/#history <https://pypi.org/project/openeo/#history>`_
- `https://github.com/Open-EO/openeo-python-client/releases <https://github.com/Open-EO/openeo-python-client/releases>`_

Here is a bash (subshell) oneliner to verify that the PyPI release works properly::

    (
        cd /tmp &&\
        python -m venv venv-openeo &&\
        source venv-openeo/bin/activate &&\
        pip install -U openeo &&\
        python -c "import openeo;print(openeo);print(openeo.__version__)"
    )

It tries to install the latest version of the ``openeo`` package in a temporary virtual env,
import it and print the package version.


Development Installation on Windows
===================================

Normally you can install the client the same way on Windows as on Linux, like so:

.. code-block:: console

    pip install -e .[dev]

Alternative development installation
-------------------------------------

The standard pure-``pip`` based installation should work with the most recent code.
However, in the past we sometimes had issues with this procedure.
Should you experience problems, consider using an alternative conda-based installation procedure:

1.  Create and activate a new conda environment for developing the openeo-python-client.
    For example:

    .. code-block:: console

        conda create -n openeopyclient
        conda activate openeopyclient

2.  In that conda environment, install only the dependencies of ``openeo`` via conda,
    but not the ``openeo`` package itself.

    .. code-block:: console

        # Install openeo dependencies (from the conda-forge channel)
        conda install --only-deps -c conda-forge openeo

3.  Do a ``pip install`` from the project root in *editable mode* (``pip -e``):

    .. code-block:: console

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

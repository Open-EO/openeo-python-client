###########################
Development and maintenance
###########################



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
-   The Python virtual environment you work in has the latests versions
    of the ``setuptools``, ``wheel`` and ``twine`` packages installed
    (as describe in the `packaging tutorial <https://packaging.python.org/tutorials/packaging-projects/#generating-distribution-archives>`_)::

        pip install --upgrade setuptools wheel twine

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

    This pre-release suffix should only be removed during the release procedure
    (and restored when bumping the version after the release procedure).

``CHANGELOG.md``
    keeps track of important changes associated with each release.
    It follows the `Keep a Changelog <https://keepachangelog.com>`_ convention
    and should be properly updated with each bug fix, feature addition/removal, ...
    under the ``Unreleased`` section during development.

Procedure
---------

These are the steps to create and publish a new release of the ``openeo`` package.
To be as concrete as possible, we will assume that we are about to release version ``0.4.7``.

#.  Make sure you are working on **latest master branch**,
    without uncommitted changes and all tests are properly passing.
#.  **Drop the pre-release suffix** from the version string in ``openeo/_version.py``
    so that it just a "final" semantic versioning string, e.g. ``0.4.7``

#.  **Update CHANGELOG.md**: rename the "Unreleased" section title
    to contain version and date, e.g. ``## [0.4.7] - 2020-12-15``
    and start a new "Unreleased" section above it.

#.  **Commit** these changes in git and **push** to GitHub
#.  Wait for **Jenkins** to build this updated master
    (trigger it manually if necessary),
    so that a build of final release ``0.4.7``
    is properly uploaded to **VITO artifactory**.

#.  Obtain a wheel archive of the package:

    -   Path of least surprise: download the built wheel
        directly from VITO artifactory, e.g.::

            curl --fail -O https://artifactory.vgt.vito.be/python-openeo/openeo/0.4.7/openeo-0.4.7-py3-none-any.whl

        This downloads ``openeo-0.4.7-py3-none-any.whl``

    -   Or, if you know what you are doing and you're sure your
        local checkout is clean without temporary source files
        all over the place, you can also build it locally::

            python setup.py sdist bdist_wheel

        This should create ``dist/openeo-0.4.7-py3-none-any.whl``

#.  **Upload** this wheel archive to PyPI::

        python -m twine upload openeo-0.4.7-py3-none-any.whl


#.  Create a **git version tag** and push it to GitHub::

        git tag v0.4.7
        git push origin v0.4.7

#.  Create a **release in GitHub**:
    Go to `https://github.com/Open-EO/openeo-python-client/releases/new <https://github.com/Open-EO/openeo-python-client/releases/new>`_,
    Enter ``v0.4.7`` under "tag", use it also as title,
    optionally describe the release a bit and publish it
    (no need to attach binaries).

#.  **Bump version** in ``openeo/_version.py``,
    and append a pre-release "a1" suffix again, for example::

        __version__ = '0.4.8a1'

    Commit this and push to GitHub.

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
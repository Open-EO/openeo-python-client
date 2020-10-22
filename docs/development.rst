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

#.  Make sure you are working on **latest master branch**,
    without uncommitted changes and all tests are properly passing.
#.  **Drop the pre-release suffix** from the version string in ``openeo/_version.py``
    so that it just a "final" semantic versioning string, e.g. ``0.4.7``

#.  **Update CHANGELOG.md**: rename the "Unreleased" section title
    to contain version and date, e.g.::

        ## [0.4.7] - 2020-12-15

    remove empty subsections
    and start a new "Unreleased" section above it, like::

        ## [Unreleased]

        ### Added

        ### Changed

        ### Removed

        ### Fixed


#.  **Commit** these changes in git with a commit message like ``Release 0.4.7``
    and **push** to GitHub
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


Profile a process server-side
=============================

Using PySPARK's profiler API it is possible to obtain profiling information of a job executed server-side.
Currently SPARK's builtin BasicProfiler is used, which runs cProfiler under the hood.
Note that this will only generate statistics over the python part of the execution, therefore it is most suitable for profiling UDFs.
However the statistics of the driver may also give insights about time spent in various Scala/Java codes.  

Usage
-----

Only batch jobs are supported! In order to turn on profiling, set 'profile' to 'true' in job options::

        job_options={'profile':'true'}
        ... # prepare the process
        process.execute_batch('result.tif',job_options=job_options)

When the process has finished, it will also download a file called 'profile_dumps.tar.gz':

-   rdd_-1.pstats is the profile data of the python driver,
-   the rest are the profiling results of the individual rdd id-s (that can be correlated with the execution using the SPARK UI).

Viewing profiling information
-----------------------------

The simplest way is to visualize the results with a graphical visualization tool called kcachegrind.
In order to do that, install `kcachegrind <http://kcachegrind.sourceforge.net/>`_ packages (most linux distributions have it installed by default) and it's python connector `pyprof2calltree <https://pypi.org/project/pyprof2calltree/>`_.
From command line run::

       pyprof2calltree rdd_<INTERESTING_RDD_ID>.pstats.

Another way is to use the builtin pstats functionality from within python::

        import pstats
		p = pstats.Stats('restats')
		p.print_stats()

Example
-------

An example code can be found `here <https://github.com/Open-EO/openeo-python-client/tree/master/examples/profiling_example.py>`_ .

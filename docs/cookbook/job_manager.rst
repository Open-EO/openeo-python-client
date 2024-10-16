====================================
Multi Backend Job Manager
====================================

API
===

.. warning::
    This is a new experimental API, subject to change.

.. autoclass:: openeo.extra.job_management.MultiBackendJobManager
    :members:

.. autoclass:: openeo.extra.job_management.JobDatabaseInterface
    :members:

.. autoclass:: openeo.extra.job_management.CsvJobDatabase

.. autoclass:: openeo.extra.job_management.ParquetJobDatabase


.. autoclass:: openeo.extra.job_management.ProcessBasedJobCreator
    :members:
    :special-members: __call__


.. _job-management-with-process-based-job-creator:

Job creation based on parameterized processes
===============================================

The openEO API supports parameterized processes out of the box,
which allows to work with flexible, reusable openEO building blocks
in the form of :ref:`user-defined processes <user-defined-processes>`
or `remote openEO process definitions <https://github.com/Open-EO/openeo-api/tree/draft/extensions/remote-process-definition>`_.
This can also be leveraged for job creation in the context of the
:py:class:`~openeo.extra.job_management.MultiBackendJobManager`:
define a "template" job as a parameterized process
and let the job manager fill in the parameters
from a given data frame.

The :py:class:`~openeo.extra.job_management.ProcessBasedJobCreator` helper class
allows to do exactly that.
Given a reference to a parameterized process,
such as a user-defined process or remote process definition,
it can be used directly as ``start_job`` callable to
:py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`
which will fill in the process parameters from the dataframe.

Basic :py:class:`~openeo.extra.job_management.ProcessBasedJobCreator` example
-----------------------------------------------------------------------------

Basic usage example with a remote process definition:

.. code-block:: python
    :linenos:
    :caption: Basic :py:class:`~openeo.extra.job_management.ProcessBasedJobCreator` example snippet
    :emphasize-lines: 10-15, 28

    from openeo.extra.job_management import (
        MultiBackendJobManager,
        create_job_db,
        ProcessBasedJobCreator,
    )

    # Job creator, based on a parameterized openEO process
    # (specified by the remote process definition at given URL)
    # which has parameters "start_date" and "bands" for example.
    job_starter = ProcessBasedJobCreator(
        namespace="https://example.com/my_process.json",
        parameter_defaults={
            "bands": ["B02", "B03"],
        },
    )

    # Initialize job database from a dataframe,
    # with desired parameter values to fill in.
    df = pd.DataFrame({
        "start_date": ["2021-01-01", "2021-02-01", "2021-03-01"],
    })
    job_db = create_job_db("jobs.csv").initialize_from_df(df)

    # Create and run job manager,
    # which will start a job for each of the `start_date` values in the dataframe
    # and use the default band list ["B02", "B03"] for the "bands" parameter.
    job_manager = MultiBackendJobManager(...)
    job_manager.run_jobs(job_db=job_db, start_job=job_starter)

In this example, a :py:class:`ProcessBasedJobCreator` is instantiated
based on a remote process definition,
which has parameters ``start_date`` and ``bands``.
When passed to :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`,
a job for each row in the dataframe will be created,
with parameter values based on matching columns in the dataframe:

-   the ``start_date`` parameter will be filled in
    with the values from the "start_date" column of the dataframe,
-   the ``bands`` parameter has no corresponding column in the dataframe,
    and will get its value from the default specified in the ``parameter_defaults`` argument.


:py:class:`~openeo.extra.job_management.ProcessBasedJobCreator` with geometry handling
---------------------------------------------------------------------------------------------

Apart from the intuitive name-based parameter-column linking,
:py:class:`~openeo.extra.job_management.ProcessBasedJobCreator`
also automatically links:

-   a process parameters that accepts inline GeoJSON geometries/features
    (which practically means it has a schema like ``{"type": "object", "subtype": "geojson"}``,
    as produced by :py:meth:`Parameter.geojson <openeo.api.process.Parameter.geojson>`).
-   with the geometry column in a `GeoPandas <https://geopandas.org/>`_ dataframe.

even if the name of the parameter does not exactly match
the name of the GeoPandas geometry column (``geometry`` by default).
This automatic liking is only done if there is only one
GeoJSON parameter and one geometry column in the dataframe.


.. admonition:: to do

        Add example with geometry handling.

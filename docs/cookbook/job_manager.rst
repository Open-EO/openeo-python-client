.. _job-manager:

====================================
Multi Backend Job Manager
====================================

The :py:class:`~openeo.extra.job_management.MultiBackendJobManager`
helps to run and manage a large number of batch jobs
across one or more openEO backends.
It handles job creation, submission, status tracking, result downloading,
error handling, and persistence of job metadata — all automatically.

It is designed for scenarios where you need to process many tasks in parallel,
for example tiling a large area of interest into smaller regions
and running a batch job for each tile.

.. contents:: On this page
    :local:
    :depth: 2


Getting Started
===============

Below is a minimal but complete example showing how to set up
the job manager, define a job creation callback, and run everything:

.. code-block:: python
    :linenos:

    import logging
    import pandas as pd
    import openeo
    from openeo.extra.job_management import MultiBackendJobManager, create_job_db

    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
    )

    # Set up the job manager and register one or more backends
    manager = MultiBackendJobManager()
    manager.add_backend("cdse", connection=openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc())

    # Define a callback that creates a batch job from a dataframe row
    def start_job(
        row: pd.Series, connection: openeo.Connection, **kwargs
    ) -> openeo.BatchJob:
        year = row["year"]
        spatial_extent = row["spatial_extent"]  # e.g. a boundig box
        cube = connection.load_collection(
            "SENTINEL2_L2A",
            spatial_extent=spatial_extent,
            temporal_extent=[f"{year}-01-01", f"{year+1}-01-01"],
            bands=["B04", "B08"],
        )
        cube = cube.ndvi(nir="B08", red="B04")
        return cube.create_job(
            title=f"NDVI {year}",
            out_format="GTiff",
        )

    # Prepare a dataframe with one row per job
    df = pd.DataFrame({"spatial_extent": ["bbox1", "bbox2", "bbox1", "bbox2"], "year": [2020, 2020, 2021, 2021]})

    # Create a persistent job database (CSV or Parquet)
    job_db = create_job_db("jobs.csv", df=df)

    # Run all jobs (this blocks until every job finishes, fails, or is canceled)
    manager.run_jobs(job_db=job_db, start_job=start_job)

The ``start_job`` callback receives a :py:class:`pandas.Series` row
and a :py:class:`~openeo.Connection` connected to one of the registered backends.
It should return a :py:class:`~openeo.BatchJob` (created but not necessarily started).
The job manager takes care of starting, polling, and downloading results.

See :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`
for the full list of parameters passed to the ``start_job`` callback.


Job Database
============

The job manager persists job metadata (status, backend, timing, costs, …)
to a **job database** so that processing can be resumed after an interruption.
Several storage backends are available.

CSV and Parquet files
---------------------

The easiest option is to use a local CSV or Parquet file.
Use the :py:func:`~openeo.extra.job_management.create_job_db` factory
to create and initialize a job database from a :py:class:`pandas.DataFrame`:

.. code-block:: python

    from openeo.extra.job_management import create_job_db

    job_db = create_job_db("jobs.csv", df=df)
    # or for Parquet:
    job_db = create_job_db("jobs.parquet", df=df)

If the file already exists (e.g. from a previous interrupted run),
you can re-open it with :py:func:`~openeo.extra.job_management.get_job_db`:

.. code-block:: python

    from openeo.extra.job_management import get_job_db

    job_db = get_job_db("jobs.csv")

and pass it directly to
:py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`
to resume where you left off.

.. tip::

    Parquet files are generally recommended over CSV for large job databases,
    as they are faster to read/write and handle data types more reliably.
    Parquet support requires the ``pyarrow`` package
    (see :ref:`optional dependencies <installation-optional-dependencies>`).

STAC API (experimental)
-----------------------

For advanced use cases, the
:py:class:`~openeo.extra.job_management.stac_job_db.STACAPIJobDatabase`
allows persisting job metadata to a STAC API service.
This is an **unstable, experimental** feature.

.. code-block:: python

    from openeo.extra.job_management.stac_job_db import STACAPIJobDatabase

    job_db = STACAPIJobDatabase(
        collection_id="my-jobs",
        stac_root_url="https://stac.example.com",
    )
    job_db.initialize_from_df(df)

Custom interfaces
-----------------

You can implement your own storage backend by subclassing
:py:class:`~openeo.extra.job_management.JobDatabaseInterface`.


Customizing Job Handling
========================

The :py:class:`~openeo.extra.job_management.MultiBackendJobManager` provides
callback methods that can be overridden to customize what happens
when a job finishes, fails, or is canceled:

-   :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.on_job_done`:
    called when a job completes successfully.
    The default implementation downloads the results and saves job metadata.

-   :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.on_job_error`:
    called when a job fails with an error.
    The default implementation saves the error logs to a JSON file.

-   :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.on_job_cancel`:
    called when a job is canceled.
    The default implementation does nothing.

Example — subclass to add custom post-processing:

.. code-block:: python

    class MyJobManager(MultiBackendJobManager):

        def on_job_done(self, job, row):
            # First, do the default download
            super().on_job_done(job, row)
            # Then add custom post-processing
            job_dir = self.get_job_dir(job.job_id)
            print(f"Results for job {job.job_id} saved to {job_dir}")

        def on_job_error(self, job, row):
            super().on_job_error(job, row)
            # e.g. send a notification
            print(f"Job {job.job_id} failed!")


Automatic Result Downloading
============================

By default, the job manager downloads results of completed jobs automatically.
This can be disabled by setting ``download_results=False``:

.. code-block:: python

    manager = MultiBackendJobManager(download_results=False)

Results and metadata are saved under the ``root_dir`` directory
(defaults to the current directory), in per-job subfolders like ``job_{job_id}/``.

.. versionadded:: 0.47.0
    The ``download_results`` parameter.


Canceling Long-Running Jobs
============================

You can set an automatic timeout for running jobs with the
``cancel_running_job_after`` parameter (in seconds).
Jobs that exceed this duration will be automatically canceled:

.. code-block:: python

    # Cancel any job that has been running for more than 2 hours
    manager = MultiBackendJobManager(cancel_running_job_after=7200)

.. versionadded:: 0.32.0


Running in a Background Thread
==============================

Instead of blocking the main thread with
:py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`,
you can run the job management loop in a background thread:

.. code-block:: python

    manager.start_job_thread(start_job=start_job, job_db=job_db)

    # ... do other work in the main thread ...

    # When done, stop the background thread
    manager.stop_job_thread()

This is useful in interactive environments such as Jupyter notebooks,
where you want to keep the main thread responsive.

.. versionadded:: 0.32.0


Job Status Tracking
===================

The job database tracks a status columns:

``status``
    The **user-visible lifecycle status**. Starts at ``"not_started"`` and
    progresses through standard openEO states (``created``, ``queued``,
    ``running``, ``finished``, ``error``, ``canceled``) as well as internal
    housekeeping states like ``queued_for_start``, ``start_failed``, and
    ``skipped``.



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

The :py:class:`~openeo.extra.job_management.process_based.ProcessBasedJobCreator` helper class
allows to do exactly that.
Given a reference to a parameterized process,
such as a user-defined process or remote process definition,
it can be used directly as ``start_job`` callable to
:py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`
which will fill in the process parameters from the dataframe.

Basic :py:class:`~openeo.extra.job_management.process_based.ProcessBasedJobCreator` example
--------------------------------------------------------------------------------------------

Basic usage example with a remote process definition:

.. code-block:: python
    :linenos:
    :caption: Basic :py:class:`~openeo.extra.job_management.process_based.ProcessBasedJobCreator` example snippet
    :emphasize-lines: 10-15, 27

    import pandas as pd
    from openeo.extra.job_management import (
        MultiBackendJobManager,
        create_job_db,
    )
    from openeo.extra.job_management.process_based import ProcessBasedJobCreator

    # Job creator, based on a parameterized openEO process
    # (specified by the remote process definition at given URL)
    # which has parameters "start_date" and "bands" for example.
    job_starter = ProcessBasedJobCreator(
        namespace="https://example.com/my_process.json",
        parameter_defaults={
            "bands": ["B02", "B03"],
        },
    )

    # Prepare a dataframe with desired parameter values to fill in.
    df = pd.DataFrame(
        {
            "start_date": ["2021-01-01", "2021-02-01", "2021-03-01"],
        }
    )

    # Create a job database initialized from the dataframe
    job_db = create_job_db("jobs.csv", df=df)

    # Create and run job manager,
    # which will start a job for each of the `start_date` values in the dataframe
    # and use the default band list ["B02", "B03"] for the "bands" parameter.
    job_manager = MultiBackendJobManager(...)
    job_manager.run_jobs(job_db=job_db, start_job=job_starter)

In this example, a :py:class:`~openeo.extra.job_management.process_based.ProcessBasedJobCreator` is instantiated
based on a remote process definition,
which has parameters ``start_date`` and ``bands``.
When passed to :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`,
a job for each row in the dataframe will be created,
with parameter values based on matching columns in the dataframe:

-   the ``start_date`` parameter will be filled in
    with the values from the "start_date" column of the dataframe,
-   the ``bands`` parameter has no corresponding column in the dataframe,
    and will get its value from the default specified in the ``parameter_defaults`` argument.


:py:class:`~openeo.extra.job_management.process_based.ProcessBasedJobCreator` with geometry handling
-----------------------------------------------------------------------------------------------------

Apart from the intuitive name-based parameter-column linking,
:py:class:`~openeo.extra.job_management.process_based.ProcessBasedJobCreator`
also automatically links:

-   a process parameter that accepts inline GeoJSON geometries/features
    (which practically means it has a schema like ``{"type": "object", "subtype": "geojson"}``,
    as produced by :py:meth:`Parameter.geojson <openeo.api.process.Parameter.geojson>`),
-   with the geometry column in a `GeoPandas <https://geopandas.org/>`_ dataframe,

even if the name of the parameter does not exactly match
the name of the GeoPandas geometry column (``geometry`` by default).
This automatic linking is only done if there is only one
GeoJSON parameter and one geometry column in the dataframe.

Example with geometry handling:

.. code-block:: python
    :linenos:
    :caption: :py:class:`~openeo.extra.job_management.process_based.ProcessBasedJobCreator` with geometry handling

    import geopandas as gpd
    from shapely.geometry import box
    from openeo.extra.job_management import MultiBackendJobManager, create_job_db
    from openeo.extra.job_management.process_based import ProcessBasedJobCreator

    # Job creator, based on a remote process definition
    # with parameters "aoi" (accepting GeoJSON) and "bands"
    job_starter = ProcessBasedJobCreator(
        namespace="https://example.com/my_ndvi_process.json",
        parameter_defaults={
            "bands": ["B04", "B08"],
        },
    )

    # Build a GeoDataFrame with geometries for each job.
    # The geometry column is automatically linked to the GeoJSON parameter.
    gdf = gpd.GeoDataFrame(
        {
            "start_date": ["2021-01-01", "2021-02-01"],
        },
        geometry=[
            box(5.0, 51.0, 5.1, 51.1),
            box(5.1, 51.1, 5.2, 51.2),
        ],
    )

    job_db = create_job_db("jobs.parquet", df=gdf)

    job_manager = MultiBackendJobManager(...)
    job_manager.run_jobs(job_db=job_db, start_job=job_starter)


API Reference
=============

.. warning::
    This is a new experimental API, subject to change.

MultiBackendJobManager
----------------------

.. autoclass:: openeo.extra.job_management.MultiBackendJobManager
    :members:

Job Database
------------

.. autoclass:: openeo.extra.job_management.JobDatabaseInterface
    :members:

.. autoclass:: openeo.extra.job_management.FullDataFrameJobDatabase
    :members: initialize_from_df

.. autoclass:: openeo.extra.job_management.CsvJobDatabase

.. autoclass:: openeo.extra.job_management.ParquetJobDatabase

.. autofunction:: openeo.extra.job_management.create_job_db

.. autofunction:: openeo.extra.job_management.get_job_db

.. autoclass:: openeo.extra.job_management.stac_job_db.STACAPIJobDatabase

ProcessBasedJobCreator
----------------------

.. autoclass:: openeo.extra.job_management.process_based.ProcessBasedJobCreator
    :members:
    :special-members: __call__

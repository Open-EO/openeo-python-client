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

.. tip::

    For hands-on, end-to-end Jupyter notebook examples, see the
    `Managing Multiple Large Scale Jobs <https://github.com/Open-EO/openeo-community-examples/tree/main/python/ManagingMultipleLargeScaleJobs>`_
    notebooks in the openEO community examples repository.
    These cover real-world workflows including job splitting, result visualization, and more.

.. contents:: On this page
    :local:
    :depth: 2


Getting Started
===============

There are three main ingredients to using the
:py:class:`~openeo.extra.job_management.MultiBackendJobManager`:

1. A **manager** with one or more registered backends.
2. A **job database** (backed by a DataFrame) that describes the work to do; one row per job.
3. A **start_job callback** that turns a single row into an openEO batch job.

The sections below walk through each of these, and then show how they
come together.

Setting up the manager
----------------------

Create a :py:class:`~openeo.extra.job_management.MultiBackendJobManager`
and register the backend you want to use.
Each backend gets a name and an authenticated connection
:py:class:`~openeo.rest.connection.Connection`:

.. code-block:: python

    import openeo
    from openeo.extra.job_management import MultiBackendJobManager

    manager = MultiBackendJobManager()
    manager.add_backend("cdse", connection=openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc())

You can register more than one backend, the manager will distribute
jobs across them automatically:

.. code-block:: python

    manager.add_backend("dev", connection=openeo.connect(
        "https://openeo-dev.example.com"
    ).authenticate_oidc())

The optional ``parallel_jobs`` argument to
:py:meth:`~openeo.extra.job_management.MultiBackendJobManager.add_backend`
controls how many jobs the manager will try to keep active simultaneously on that backend (default: 2).
This is the manager's own limit, independent of the backend's infrastructure limits.
The actual number of jobs that can run in parallel depends on the backend's capacity per user.

Preparing the job database
--------------------------

The job database is a :py:class:`pandas.DataFrame` where **each row
represents one job** you want to run. The columns hold the parameters
your ``start_job`` callback will read for example a year, a spatial
extent, a file path, etc.

Wrap the DataFrame in a persistent job database
(CSV or Parquet) so progress is saved to disk and can be resumed if
interrupted:

.. code-block:: python

    import pandas as pd
    from openeo.extra.job_management import create_job_db

    df = pd.DataFrame({
        "spatial_extent": [
            {"west": 5.0, "south": 51.0, "east": 5.1, "north": 51.1},
            {"west": 5.1, "south": 51.1, "east": 5.2, "north": 51.2},
        ],
        "year": [2021, 2022],
    })
    job_db = create_job_db("jobs.csv", df=df)

The manager will automatically add bookkeeping columns
(``status``, ``id``, ``backend_name``, ``start_time``, …),
you only need to supply the columns relevant to your processing.

Defining the start_job callback
-------------------------------

The ``start_job`` callback is a function you write. It receives a
:py:class:`pandas.Series` (one row of the DataFrame) and a
:py:class:`~openeo.rest.connection.Connection`, and should return
a :py:class:`~openeo.rest.job.BatchJob`:

.. code-block:: python

    def start_job(row, connection, **kwargs):
        cube = connection.load_collection(
            "SENTINEL2_L2A",
            spatial_extent=row["spatial_extent"],
            temporal_extent=[f"{row['year']}-01-01", f"{row['year']+1}-01-01"],
            bands=["B04", "B08"],
        )
        cube = cube.ndvi(nir="B08", red="B04")
        return cube.create_job(
            title=f"NDVI {row['year']}",
            out_format="GTiff",
        )

A few things to note:

- The callback should **create** the job (``create_job``), but does not
  need to **start** it, the manager takes care of that.
- Always include ``**kwargs`` so the manager can pass extra arguments
  (like ``provider``, ``connection_provider``) without causing errors.
- You can read any column you put in the DataFrame via ``row["..."]``.

See :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs`
for the full list of parameters passed to the callback.

Running everything
------------------

With all three pieces in place, a single call kicks off the processing
loop. It blocks until every job has finished, failed, or been canceled:

.. code-block:: python

    import logging

    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
    )

    manager.run_jobs(job_db=job_db, start_job=start_job)

Enabling logging (as shown above) is highly recommended — the manager
logs status changes, retries, and errors so you can follow progress.


Job Database
============

The job manager persists job metadata (status, backend, timing, costs, …)
to a **job database** so that processing can be resumed after an interruption.
Several storage backends are available.

CSV and Parquet files
---------------------

The easiest option is to use a local CSV or Parquet file.
Use the :py:func:`~openeo.extra.job_management.create_job_db` factory
to create and initialize a job database from a :py:class:`pandas.DataFrame` or a :py:class:`geopandas.GeoDataFrame `:

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
See the :ref:`API reference below <job-manager-api-reference>` for the full interface.


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


By default, :py:meth:`~openeo.extra.job_management.MultiBackendJobManager.run_jobs` blocks the main thread until all jobs are finished, failed, or canceled.
To keep your main program responsive (e.g., in a Jupyter notebook or GUI), run the job manager loop in a background thread so you can still monitor or interact with for instance the dataframe.
.. code-block:: python

    manager.start_job_thread(start_job=start_job, job_db=job_db)

    # ... do other work in the main thread ...
    # For example, you can monitor job_db, update a UI, or submit new jobs.

    # When done, stop the background thread
    manager.stop_job_thread()

While the background thread is running, you can inspect the job database (e.g., with pandas or geopandas) to monitor progress, or perform other tasks in your main program. This is especially useful in interactive environments where you want to avoid blocking the UI or kernel.

**Caveats:**

- The background thread will keep running until all jobs are finished, failed, or canceled, or until you call ``stop_job_thread()``.
- Logging output from the background thread will still appear in the console.

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


.. _job-manager-api-reference:

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

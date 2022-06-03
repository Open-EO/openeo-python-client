.. _batch_jobs:

============
Batch Jobs
============

Most of the simple, basic openEO usage examples show **synchronous** downloading of results:
you submit a process graph with a (HTTP POST) request and receive the result
as direct response of that same request.
This only works properly if the processing doesn't take too long (order of seconds, or a couple of minutes at most).

For the heavier work (larger regions of interest, larger time series, more intensive processing, ...)
you have to use **batch jobs**, which are supported in the openEO API through separate HTTP requests, corresponding to these steps:

- you create a job (providing a process graph and some other metadata like title, description, ...)
- you start the job
- you wait for the job to finish, periodically polling its status
- when the job finished successfully: get the listing of result assets
- you download the result assets (or use them in an other way)


Create a batch job
===================

In the openEO Python Client Library, if you have a (raster) data cube, you can easily
create a batch job with the :py:meth:`DataCube.create_job() <openeo.rest.datacube.DataCube.create_job>` method.
It's important to specify in what format the result should be stored,
which can be done with an explicit :py:meth:`DataCube.save_result() <openeo.rest.datacube.DataCube.save_result>` call before creating the job:

.. code-block:: python

    cube = connection.load_collection(...)
    ...
    cube = cube.save_result(format="GTiff")
    job = cube.create_job()

or directly in :py:meth:`~openeo.rest.datacube.DataCube.create_job()`:

.. code-block:: python

    cube = connection.load_collection(...)
    ...
    job = cube.create_job(out_format="GTiff)

While not necessary, it is also recommended to give your batch job a descriptive title
so it's easier to identify in your job listing, e.g.:

.. code-block:: python

    job = cube.create_job(title="NDVI timeseries 2022")


Batch job object
-----------------

The ``job`` object returned by :py:meth:`~openeo.rest.datacube.DataCube.create_job()`
is a :py:class:`~openeo.rest.job.BatchJob` object.
It is basically a client-side reference to a batch job that exists on the back-end
and allows to interact with that batch job.
A batch job on a back-end is fully identified by its
:py:data:`~openeo.rest.job.BatchJob.job_id`:

.. code-block:: pycon

    >>> job.job_id
    '3461f51b-c12b-481d-9a0a-d50526933183'

.. note::
    The :py:class:`~openeo.rest.job.BatchJob` class originally had
    the more cryptic name :py:class:`~openeo.rest.job.RESTJob`,
    which is still available as legacy alias,
    but :py:class:`~openeo.rest.job.BatchJob` is recommended since version 0.11.0.


Reconnecting to a batch job
----------------------------

Say you already have a batch job on the back-end, for example created at another
time, in another script/notebook or even with another openEO client.
If you have the batch job id, you can "reconnect" to that batch job
by creating a :py:class:`~openeo.rest.job.BatchJob` object
using :py:meth:`Connection.job() <openeo.rest.connection.Connection.job>`:

.. code-block:: python

    job_id = "5d806224-fe79-4a54-be04-90757893795b"
    job = connection.job(job_id)



Start a batch job
=================

Starting a batch job is pretty straightforward with the
:py:meth:`~openeo.rest.job.BatchJob.start_job()` method:

.. code-block:: python

    job.start_job()

If this didn't raise any errors or exceptions your job
should now have started (status "running")
or at least be queued for processing (status "queued").


Wait for a batch job to finish
==============================

A batch job typically takes some time to finish,
and you can check its status with the :py:meth:`~openeo.rest.job.BatchJob.status()` method:

.. code-block:: pycon

    >>> job.status()
    "running"

The possible batch job status values, defined by the openEO API, are
"created", "queued", "running", "canceled", "finished" and "error".

Usually, you can only reliably get results from your job
when it reaches status "finished".

Create, start and wait in one go
=================================

You could, depending on your situation, manually check your job's status periodically
or set up a polling loop system to keep an eye on your job.
The openEO Python client library also provides helpers to do that for you.

If you have a batch job that is already created as shown above, you can use
the :py:meth:`~openeo.rest.job.BatchJob.start_and_wait()` method
to start it and periodically poll its status until it reaches status "finished" (or fails with status "error").
Along the way it will print some progress messages.

.. code-block:: pycon

    >>> job.start_and_wait()
    0:00:00 Job 'b0e8adcf-087f-41de-afe6-b3c0ea88ff38': send 'start'
    0:00:36 Job 'b0e8adcf-087f-41de-afe6-b3c0ea88ff38': queued (progress N/A)
    0:01:35 Job 'b0e8adcf-087f-41de-afe6-b3c0ea88ff38': queued (progress N/A)
    0:02:19 Job 'b0e8adcf-087f-41de-afe6-b3c0ea88ff38': running (progress N/A)
    0:02:50 Job 'b0e8adcf-087f-41de-afe6-b3c0ea88ff38': running (progress N/A)
    0:03:28 Job 'b0e8adcf-087f-41de-afe6-b3c0ea88ff38': finished (progress N/A)


If you didn't create the batch job yet from a given :py:class:`~openeo.rest.datacube.DataCube`
you can do the job creation, starting and waiting on one go
with :py:meth:`~openeo.rest.datacube.DataCube.execute_batch()`:

.. code-block:: pycon

    >>> job = cube.execute_batch()
    0:00:00 Job 'f9f4e3d3-bc13-441b-b76a-b7bfd3b59669': send 'start'
    0:00:23 Job 'f9f4e3d3-bc13-441b-b76a-b7bfd3b59669': queued (progress N/A)
    ...


Monitoring and debugging
=========================

TODO

Download batch job results
==========================

Once a batch job is finished you can get a handle to the results
(which can be a single file or multiple files) and metadata
with :py:meth:`~openeo.rest.job.BatchJob.get_results` ::

    >>> results = job.get_results()
    >>> results
    <JobResults for job '57da31da-7fd4-463a-9d7d-c9c51646b6a4'>

The result metadata describes the spatio-temporal properties of the result
and is in fact a valid STAC item::

    >>> results.get_metadata()
    {
        'bbox': [3.5, 51.0, 3.6, 51.1],
        'geometry': {'coordinates': [[[3.5, 51.0], [3.5, 51.1], [3.6, 51.1], [3.6, 51.0], [3.5, 51.0]]], 'type': 'Polygon'},
        'assets': {
            'res001.tiff': {
                'href': 'https://openeo.example/download/432f3b3ef3a.tiff',
                'type': 'image/tiff; application=geotiff',
                ...
            'res002.tiff': {
                ...


Download all assets
--------------------

In the general case, when you have one or more result files (also called "assets"),
the easiest option to download them is
using :py:meth:`~openeo.rest.job.JobResults.download_files` (plural)
where you just specify a download folder
(otherwise the current working directory will be used by default)::

    results.download_files("data/out")

The resulting files will be named as they are advertised in the results metadata
(e.g. ``res001.tiff`` and ``res002.tiff`` in case of the metadata example above).


Download single asset
---------------------

If you know that there is just a single result file, you can also download it directly with
:py:meth:`~openeo.rest.job.JobResults.download_file` (singular) with the desired file name::

    results.download_file("data/out/result.tiff")

This will fail however if there are multiple assets in the job result
(like in the metadata example above).
In that case you can still download a single by specifying which one you
want to download with the ``name`` argument::

    result.download_file("data/out/result.tiff", name="res002.tiff")


Fine-grained asset downloads
----------------------------

If you need a bit more control over which asset to download and how,
you can iterate over the result assets explicitly
and download these :py:class:`~openeo.rest.job.ResultAsset` instances
with :py:meth:`~openeo.rest.job.ResultAsset.download`, like this::

    for asset in results.get_assets():
        if asset.metadata["type"].startswith("image/tiff"):
            asset.download("data/out/result-v2-" + asset.name)


Directly load batch job results
===============================

If you want to skip downloading an asset to disk, you can also load it directly.
For example, load a JSON asset with :py:meth:`~openeo.rest.job.ResultAsset.load_json`::

    >>> asset.metadata
    {"type": "application/json", "href": "https://openeo.example/download/432f3b3ef3a.json"}
    >>> data = asset.load_json()
    >>> data
    {"2021-02-24T10:59:23Z": [[3, 2, 5], [3, 4, 5]], ....}


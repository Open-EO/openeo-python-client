
.. index::
    single: batch job
    see: job; batch job

.. _batch-jobs-chapter:

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

.. tip::

    This documentation mainly discusses how to **programmatically**
    create and interact with batch job using the openEO Python client library.
    The openEO API however does not enforce usage of the same tool
    for each step in the batch job life cycle.

    For example: if you prefer a graphical, web-based **interactive environment**
    to manage and monitor your batch jobs,
    feel free to *switch to an openEO web editor*
    like `editor.openeo.org <https://editor.openeo.org/>`_
    or `editor.openeo.cloud <https://editor.openeo.cloud/>`_
    at any time.
    After logging in with the same account you use in your Python scripts,
    you should see your batch jobs listed under the "Data Processing" tab:

    .. image:: _static/images/batchjobs-webeditor-listing.png

    With the "action" buttons on the right, you can for example
    inspect batch job details, start/stop/delete jobs,
    download their results, get batch job logs, etc.



.. index:: batch job; create

Create a batch job
===================

In the openEO Python Client Library, if you have a (raster) data cube, you can easily
create a batch job with the :py:meth:`DataCube.create_job() <openeo.rest.datacube.DataCube.create_job>` method.
It's important to specify in what *format* the result should be stored,
which can be done with an explicit :py:meth:`DataCube.save_result() <openeo.rest.datacube.DataCube.save_result>` call before creating the job:

.. code-block:: python

    cube = connection.load_collection(...)
    ...
    # Store raster data as GeoTIFF files
    cube = cube.save_result(format="GTiff")
    job = cube.create_job()

or directly in :py:meth:`job.create_job() <openeo.rest.datacube.DataCube.create_job>`:

.. code-block:: python

    cube = connection.load_collection(...)
    ...
    job = cube.create_job(out_format="GTiff)

While not necessary, it is also recommended to give your batch job a descriptive title
so it's easier to identify in your job listing, e.g.:

.. code-block:: python

    job = cube.create_job(title="NDVI timeseries 2022")



.. index:: batch job; object

Batch job object
=================

The ``job`` object returned by :py:meth:`~openeo.rest.datacube.DataCube.create_job()`
is a :py:class:`~openeo.rest.job.BatchJob` object.
It is basically a *client-side reference* to a batch job that *exists on the back-end*
and allows to interact with that batch job
(see the :py:class:`~openeo.rest.job.BatchJob` API docs for
available methods).


.. note::
    The :py:class:`~openeo.rest.job.BatchJob` class originally had
    the more cryptic name :py:class:`~openeo.rest.job.RESTJob`,
    which is still available as legacy alias,
    but :py:class:`~openeo.rest.job.BatchJob` is (available and) recommended since version 0.11.0.


A batch job on a back-end is fully identified by its
:py:data:`~openeo.rest.job.BatchJob.job_id`:

.. code-block:: pycon

    >>> job.job_id
    'd5b8b8f2-74ce-4c2e-b06d-bff6f9b14b8d'


Reconnecting to a batch job
----------------------------

Depending on your situation or use case:
make sure to properly take note of the batch job id.
It allows you to "reconnect" to your job on the back-end,
even if it was created at another time,
by another script/notebook or even with another openEO client.

Given a back-end connection and the batch job id,
use :py:meth:`Connection.job() <openeo.rest.connection.Connection.job>`
to create a :py:class:`~openeo.rest.job.BatchJob` object for an existing batch job:

.. code-block:: python

    job_id = "5d806224-fe79-4a54-be04-90757893795b"
    job = connection.job(job_id)


Jupyter integration
--------------------

:py:class:`~openeo.rest.job.BatchJob` objects have basic Jupyter notebook integration.
Put your :py:class:`~openeo.rest.job.BatchJob` object as last statement
in a notebook cell and you get an overview of your batch jobs,
including job id, status, title and even process graph visualization:

.. image:: _static/images/batchjobs-jupyter-created.png


.. index:: batch job; listing

List your batch jobs
========================

You can list your batch jobs on the back-end with
:py:meth:`Connection.list_jobs() <openeo.rest.connection.Connection.list_jobs>`, which returns a list of job metadata:

.. code-block:: pycon

    >>> connection.list_jobs()
    [{'title': 'NDVI timeseries 2022', 'status': 'created', 'id': 'd5b8b8f2-74ce-4c2e-b06d-bff6f9b14b8d', 'created': '2022-06-08T08:58:11Z'},
     {'title': 'NDVI timeseries 2021', 'status': 'finished', 'id': '4e720e70-88bd-40bc-92db-a366985ebd67', 'created': '2022-06-04T14:46:06Z'},
     ...

The listing returned by :py:meth:`Connection.list_jobs() <openeo.rest.connection.Connection.list_jobs>`
has Jupyter notebook integration:

.. image:: _static/images/batchjobs-jupyter-listing.png


.. index:: batch job; start

Run a batch job
=================

Starting a batch job is pretty straightforward with the
:py:meth:`~openeo.rest.job.BatchJob.start()` method:

.. code-block:: python

    job.start()

If this didn't raise any errors or exceptions your job
should now have started (status "running")
or be queued for processing (status "queued").



.. index:: batch job; status

Wait for a batch job to finish
--------------------------------

A batch job typically takes some time to finish,
and you can check its status with the :py:meth:`~openeo.rest.job.BatchJob.status()` method:

.. code-block:: pycon

    >>> job.status()
    "running"

The possible batch job status values, defined by the openEO API, are
"created", "queued", "running", "canceled", "finished" and "error".

Usually, you can only reliably get results from your job,
as discussed in :ref:`batch_job_results`,
when it reaches status "finished".



.. index:: batch job; polling loop

Create, start and wait in one go
----------------------------------

You could, depending on your situation, manually check your job's status periodically
or set up a **polling loop** system to keep an eye on your job.
The openEO Python client library also provides helpers to do that for you.

Working from an existing :py:class:`~openeo.rest.job.BatchJob` instance

    If you have a batch job that is already created as shown above, you can use
    the :py:meth:`job.start_and_wait() <openeo.rest.job.BatchJob.start_and_wait>` method
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


Working from a :py:class:`~openeo.rest.datacube.DataCube` instance

    If you didn't create the batch job yet from a given :py:class:`~openeo.rest.datacube.DataCube`
    you can do the job creation, starting and waiting in one go
    with :py:meth:`cube.execute_batch() <openeo.rest.datacube.DataCube.execute_batch>`:

    .. code-block:: pycon

        >>> job = cube.execute_batch()
        0:00:00 Job 'f9f4e3d3-bc13-441b-b76a-b7bfd3b59669': send 'start'
        0:00:23 Job 'f9f4e3d3-bc13-441b-b76a-b7bfd3b59669': queued (progress N/A)
        ...

    Note that :py:meth:`cube.execute_batch() <openeo.rest.datacube.DataCube.execute_batch>`
    returns a :py:class:`~openeo.rest.job.BatchJob` instance pointing to
    the newly created batch job.


.. tip::

    You can fine-tune the details of the polling loop (the poll frequency,
    how the progress is printed, ...).
    See :py:meth:`job.start_and_wait() <openeo.rest.job.BatchJob.start_and_wait>`
    or :py:meth:`cube.execute_batch() <openeo.rest.datacube.DataCube.execute_batch>`
    for more information.


.. index:: batch job; logs


.. _batch-job-logs:

Batch job logs
===============

Batch jobs in openEO have **logs** to help with *monitoring and debugging* batch jobs.
The back-end typically uses this to dump information during data processing
that may be relevant for the user (e.g. warnings, resource stats, ...).
Moreover, openEO processes like ``inspect`` allow users to log their own information.

Batch job logs can be fetched with :py:meth:`job.logs() <openeo.rest.job.BatchJob.logs>`

.. code-block:: pycon

    >>> job.logs()
    [{'id': 'log001', 'level': 'info', 'message': 'Job started with 4 workers'},
     {'id': 'log002', 'level': 'debug', 'message': 'Loading 5x3x6 tiles'},
     {'id': 'log003', 'level': 'error', 'message': "Failed to load data cube: corrupt data for tile 'J9A7K2'."},
    ...

In a Jupyter notebook environment, this also comes with Jupyter integration:

.. image:: _static/images/batchjobs-jupyter-logs.png



Automatic batch job log printing
---------------------------------

When using
:py:meth:`job.start_and_wait() <openeo.rest.job.BatchJob.start_and_wait>`
or :py:meth:`cube.execute_batch() <openeo.rest.datacube.DataCube.execute_batch>`
to run a batch job and it fails,
the openEO Python client library will automatically
print the batch job logs and instructions to help with further investigation:

.. code-block:: pycon

    >>> job.start_and_wait()
    0:00:00 Job '68caccff-54ee-470f-abaa-559ed2d4e53c': send 'start'
    0:00:01 Job '68caccff-54ee-470f-abaa-559ed2d4e53c': running (progress N/A)
    0:00:07 Job '68caccff-54ee-470f-abaa-559ed2d4e53c': error (progress N/A)

    Your batch job '68caccff-54ee-470f-abaa-559ed2d4e53c' failed.
    Logs can be inspected in an openEO (web) editor
    or with `connection.job('68caccff-54ee-470f-abaa-559ed2d4e53c').logs()`.

    Printing logs:
    [{'id': 'log001', 'level': 'info', 'message': 'Job started with 4 workers'},
    {'id': 'log002', 'level': 'debug', 'message': 'Loading 5x3x6 tiles'},
    {'id': 'log003', 'level': 'error', 'message': "Failed to load data cube: corrupt data for tile 'J9A7K2'."}]



.. index:: batch job; results

.. _batch_job_results:

Download batch job results
==========================

Once a batch job is finished you can get a handle to the results
(which can be a single file or multiple files) and metadata
with :py:meth:`~openeo.rest.job.BatchJob.get_results`:

.. code-block:: pycon

    >>> results = job.get_results()
    >>> results
    <JobResults for job '57da31da-7fd4-463a-9d7d-c9c51646b6a4'>

The result metadata describes the spatio-temporal properties of the result
and is in fact a valid STAC item:

.. code-block:: pycon

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
(otherwise the current working directory will be used by default):

.. code-block:: python

    results.download_files("data/out")

The resulting files will be named as they are advertised in the results metadata
(e.g. ``res001.tiff`` and ``res002.tiff`` in case of the metadata example above).


Download single asset
---------------------

If you know that there is just a single result file, you can also download it directly with
:py:meth:`~openeo.rest.job.JobResults.download_file` (singular) with the desired file name:

.. code-block:: python

    results.download_file("data/out/result.tiff")

This will fail however if there are multiple assets in the job result
(like in the metadata example above).
In that case you can still download a single by specifying which one you
want to download with the ``name`` argument:

.. code-block:: python

    results.download_file("data/out/result.tiff", name="res002.tiff")


Fine-grained asset downloads
----------------------------

If you need a bit more control over which asset to download and how,
you can iterate over the result assets explicitly
and download these :py:class:`~openeo.rest.job.ResultAsset` instances
with :py:meth:`~openeo.rest.job.ResultAsset.download`, like this:

.. code-block:: python

    for asset in results.get_assets():
        if asset.metadata["type"].startswith("image/tiff"):
            asset.download("data/out/result-v2-" + asset.name)


Directly load batch job results
===============================

If you want to skip downloading an asset to disk, you can also load it directly.
For example, load a JSON asset with :py:meth:`~openeo.rest.job.ResultAsset.load_json`:

.. code-block:: pycon

    >>> asset.metadata
    {"type": "application/json", "href": "https://openeo.example/download/432f3b3ef3a.json"}
    >>> data = asset.load_json()
    >>> data
    {"2021-02-24T10:59:23Z": [[3, 2, 5], [3, 4, 5]], ....}

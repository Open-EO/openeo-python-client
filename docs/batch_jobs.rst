.. _batch_jobs:

============
Batch Jobs
============

TODO


Download batch job results
--------------------------

Once a batch job is finished you can get a handle to the results
(which can be a single file or multiple files) and metadata
with :py:meth:`openeo.rest.job.RESTJob.get_results` ::

    results = job.get_results()

The result metadata describes the spatio-temporal properties of the result
and is in fact a valid STAC item::

    >>> print(results.get_metadata())
    {
        'bbox': [3.5, 51.0, 3.6, 51.1],
        'geometry': {'coordinates': [[[3.5, 51.0], [3.5, 51.1], [3.6, 51.1], [3.6, 51.0], [3.5, 51.0]]], 'type': 'Polygon'},
        'assets': ....

In the general case, when you have one or more result files (also called "assets"),
the easiest option to download them is with :py:meth:`openeo.rest.job.JobResults.download_files`
where you just specify a download folder::

    results.download_files("data/results")

The resulting files will be named as they are advertised in the results metadata.

If you know that there is just a single result file, you can also download it directly with
:py:meth:`openeo.rest.job.JobResults.download_file` with the desired file name::

    results.download_file("data/result.tiff")

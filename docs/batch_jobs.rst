.. _batch_jobs:

============
Batch Jobs
============

TODO


Download batch job results
==========================

Once a batch job is finished you can get a handle to the results
(which can be a single file or multiple files) and metadata
with :py:meth:`~openeo.rest.job.RESTJob.get_results` ::

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


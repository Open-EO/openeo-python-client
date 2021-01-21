===========
Basic Usage
===========

.. automodule:: openeo

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Example: Simple band math
-------------------------
A common task in earth observation, is to apply a formula to a number of bands
in order to compute an 'index', such as NDVI, NDWI, EVI, ...

Begin by importing the openeo module::

    import openeo

Now we need to connect to a backend::

    connection = openeo.connect('https://openeo.vito.be')

Now, we have a :class:`Connection <openeo.Connection>` object called ``connection``.
This is our entry point to the backend and allows us to discover its capabilities and collections programmatically.
Use the openEO Hub (http://hub.openeo.org/) to explore a backend
in a more graphical interactive way.

Band math usually starts from a raster data cube, with multiple spectral bands available.
The backend used here has a Sentinel-2 collection: TERRASCOPE_S2_TOC_V2::

    sentinel2_data_cube = connection.load_collection(
        "TERRASCOPE_S2_TOC_V2",
        spatial_extent={"west": 5.1518, "east": 5.1533, "south": 51.1819, "north": 51.1846, "crs": 4326},
        temporal_extent=["2016-01-01", "2016-03-10"],
        bands=["TOC-B02_10M", "TOC-B04_10M", "TOC-B08_10M"]
    )

.. note::
   Note how we specify a time range and set of bands to load. By filtering as early as possible, we avoid
   incurring unneeded costs, and make it easier for the backend to load the right data.

Now we have a :class:`ImageCollection <openeo.ImageCollection>` object called ``sentinel2_data_cube``.
Creating this object does not yet load any data, but virtually it can contain a lot of data depending on the filters you
specified.

On this data cube, we can now select the individual bands::

    B02 = sentinel2_data_cube.band("TOC-B02_10M")
    B04 = sentinel2_data_cube.band("TOC-B04_10M")
    B08 = sentinel2_data_cube.band("TOC-B08_10M")

In this example, we'll compute the enhanced vegetation index (EVI)::

    evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
    evi_cube.download("out.geotiff", format="GTiff")


Some results take a longer time to compute, in that case, the 'download' method used above may result in a timeout.
To prevent that, it is also possible to use a 'batch' job.
An easy way to run a batch job and downloading the result is::

    evi_cube.execute_batch("out.geotiff", out_format="GTiff")

This method will wait until the result is generated, which may take quite a long time. Use the batch job API if you want to
manage your jobs directly.

Managing jobs in openEO
#######################
There are 2 ways to get a result in openEO: either by retrieving it directly, which only works if the result
is computed relatively fast, usually this means in a few minutes max.
In other cases, you will need a 'batch job'.
Once submitted, the client can check the status of the batch job on a regular basis, and the results can be retrieved when it's ready.

For basic usage, the recommended approach to batch jobs is to use this all-in-one call::

    evi_cube.execute_batch("out.geotiff", out_format="GTiff")

This will start your job, wait for it to finish, and download the result. One very important thing to note,
is that your application may stop unexpectedly before your job finishes (for instance if you machine decides to reboot).
In that case, your job will not be lost, and can be managed with the commands below.

When running a batch job, it is sometimes necessary to cancel it, or to manually retrieve status information.

Usually, you first need to get hold of your job, this can be done through a job id
(an opaque string like for example ``0915ed2c-44a0-4519-8949-c58176ed2859``)
which is displayed when launching the job through a call like ``execute_batch`` above.
In a separate/new Python session, you can then inspect this job::

    import openeo
    connection = openeo.connect("https://openeo.vito.be").authenticate_basic("your_user", "your_password")
    my_job = connection.job("0915ed2c-44a0-4519-8949-c58176ed2859")
    my_job.describe_job()


If the job has finished, you can download results::

    my_job.download_results("my_results.tiff")


Stopping your job
*****************
Use this simple call to stop your job::

    my_job.stop_job()

Logs can also be retrieved, this is mostly relevant in case your job failed::

    log_list = my_job.logs()
    log_list[0].message

Restarting a job
****************
A job can also be restarted, for instance if an earlier run was aborted::

    import openeo
    connection = openeo.connect("https://openeo.vito.be").authenticate_basic("your_user","your_password")
    my_job = connection.job("da34492c-4f9d-402b-a5e9-11b528eaa152")
    my_job.start_job()


Example: Applying a mask
------------------------
It is very common for earth observation data to have separate masking layers that for instance indicate
whether a pixel is covered by a (type of) cloud or not. For Sentinel-2, one such layer is the 'scene classification'
layer that is generated by the Sen2Cor algorithm. In this example, we will use this layer to mask clouds out of our data.

First we load data, and create a binary mask. Vegetation pixels have a value of '4' in the scene classification, so we set these
pixels to 0 and all other pixels to 1 using a simple comparison::

    s2_sceneclassification = (
        connection.load_collection("TERRASCOPE_S2_TOC_V2", bands=["SCENECLASSIFICATION_20M"])
        .filter_temporal(extent=["2016-01-01", "2016-03-10"])
        .filter_bbox(west=5.1518, east=5.1533,south=51.1819,north=51.1846, crs=4326)
        .band("SCENECLASSIFICATION_20M")
    )

    mask = (s2_sceneclassification != 4)

Once the mask is created, it can be applied to the cube::

    evi_cube_masked = evi_cube.mask(mask.resample_cube_spatial(evi_cube))

Example: Retrieving aggregated timeseries
-----------------------------------------
A common type of analysis is aggregating pixel values over one or more regions of interest.
This is also referred to as 'zonal statistics'. This library has a number of predefined methods
for various types of aggregations.
In this example, we'll show how to compute an aggregated NDVI value,
using :func:`~openeo.rest.connection.DataCube.polygonal_mean_timeseries`
with the region of interest given as Shapely (multi)polygon object ::

    timeseries_dict = (
        connection.load_collection(
            "TERRASCOPE_S2_TOC_V2",
            temporal_extent = ["2020-01-01", "2020-03-10"],
            spatial_extent=dict(zip(["west", "south", "east", "north"], bbox)),
            bands=["TOC-B04_10M","TOC-B08_10M"]
        )
        .ndvi()
        .polygonal_mean_timeseries(polygon)
        .execute()
    )

The result is a dictionary object containing values for each polygon and band.
It can easily be converted into a pandas dataframe::

    import pandas as pd
    from openeo.rest.conversions import timeseries_json_to_pandas
    dataframe = timeseries_json_to_pandas(timeseries_dict)
    dataframe.index = pd.to_datetime(dataframe.index)
    dataframe.dropna().plot(title='openEO NDVI with clouds')

.. image:: images/timeseries.png
  :width: 400
  :alt: plotted timeseries

The same method also works for multiple polygons, or GeoJSON or SHP files that are
accessible by the backend. This allows computing aggregated values over very large areas.

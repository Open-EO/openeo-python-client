================
Getting Started
================


Connect to an openEO back-end
-------------------------------

First, establish a connection to an openEO back-end, using its connection URL.
For example the VITO/Terrascope backend:

.. code-block:: python

    import openeo

    connection = openeo.connect("openeo.vito.be")

The resulting :py:class:`~openeo.rest.connection.Connection` object is your central gateway to

- list data collections, available processes, file formats and other capabilities of the back-end
- start building your openEO algorithm from the desired data on the back-end
- execute and monitor (batch) jobs on the back-end
- etc.

.. seealso::

    Use the `openEO Hub <http://hub.openeo.org/>`_ to explore different back-end options
    and their capabilities in a web-based way.


Collection discovery
---------------------

The Earth observation data (the input of your openEO jobs) is organised in
`so-called collections <https://openeo.org/documentation/1.0/glossary.html#eo-data-collections>`_,
e.g. fundamental satellite collections like "Sentinel 1" or "Sentinel 2",
or preprocessed collections like "NDVI".

You can programmatically list the collections that are available on a back-end
and their metadata using methods on the `connection` object we just created
(like :py:meth:`~openeo.rest.connection.Connection.list_collection_ids`
or :py:meth:`~openeo.rest.connection.Connection.describe_collection`

.. code-block:: pycon

    >>> # Get all collection ids
    >>> connection.list_collection_ids()
    ['SENTINEL1_GRD', 'SENTINEL2_L2A', ...

    >>> # Get metadata of a single collection
    >>> connection.describe_collection("SENTINEL2_L2A")
    {'id': 'SENTINEL2_L2A', 'title': 'Sentinel-2 top of canopy ...', 'stac_version': '0.9.0', ...

Congrats, you now just did your first real openEO queries to the openEO back-end
using the openEO Python client library.

.. seealso::

    Find out more about data discovery, loading and filtering at :ref:`data_access_chapter`.


Authentication
---------------

In the code snippets above we did not need to log in as a user
since we just queried publicly available back-end information.
However, to run non-trivial processing queries one has to authenticate
so that permissions, resource usage, etc. can be managed properly.

To handle authentication, openEO leverages `OpenID Connect (OIDC) <https://openid.net/connect/>`_.
It offers some interesting features (e.g. a user can securely reuse an existing account),
but is a fairly complex topic, discussed in more depth at :ref:`authentication_chapter`.

The openEO Python client library tries to make authentication as streamlined as possible.
In most cases for example, the following snippet is enough to obtain an authenticated connection:

.. code-block:: python

    import openeo

    connection = openeo.connect("openeo.vito.be").authenticate_oidc()

This statement will automatically reuse a previously authenticated session, when available.
Otherwise, e.g. the first time you do this, some user interaction is required
and it will print a web link and a short *user code*.
Visit this web page in a browser, log in there with an existing account and enter the user code.
If everything goes well, the ``connection`` object in the script will be authenticated
and the back-end will be able to identify you in subsequent requests.




Example: Simple band math
-------------------------

A common task in earth observation is to apply a formula to a number of bands
in order to compute an 'index', such as NDVI, NDWI, EVI, ...


Band math usually starts from a raster data cube, with multiple spectral bands available.
The back-end used here has a Sentinel-2 collection: SENTINEL2_L2A:

.. code-block:: python

    sentinel2_data_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={"west": 5.15, "south": 51.181, "east": 5.155, "north": 51.184},
        temporal_extent=["2016-01-01", "2016-03-10"],
        bands=["B02", "B04", "B08"]
    )

.. note::
    Note how we specify a the region of interest, a time range and a set of bands to load.
    By filtering as early as possible, we make sure the back-end only loads the
    data we are interested in and avoid incurring unneeded costs.

Now we have a :py:class:`~openeo.rest.datacube.DataCube` object called ``sentinel2_data_cube``.
We just created a client-side reference here and did not actually load any real data.
This will only happen at the back-end once we explicitly execute the data processing
pipeline we are building.

On this data cube, we can now select the individual bands
(and rescale the digital number values to physical reflectances):

.. code-block:: python

    blue = sentinel2_data_cube.band("B02") * 0.0001
    red = sentinel2_data_cube.band("B04") * 0.0001
    nir = sentinel2_data_cube.band("B08") * 0.0001

In this example, we'll compute the enhanced vegetation index (EVI):

.. code-block:: python

    evi_cube = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)

It's important to note that, while this looks like an actual calculation,
there is no real data processing going on here.
The ``evi_cube`` object at this point is just an abstract representation
of the algorithm we want to execute.

Let's download this as a GeoTIFF file,
Because GeoTIFF does not support a temporal dimension,
we first eliminate it by taking the temporal maximum value for each pixel:


    evi_composite = evi_cube.max_time()

Now we can download this to a local file:

.. code-block:: python

    evi_composite.download("evi_composite.tiff", format="GTiff")

It's this synchronous download that triggers actual processing on the back-end,
which normally should take a couple of seconds to return.


Some results take a longer time to compute and in that case,
the 'download' method used above may result in a timeout.
To prevent that, it is also possible to use a 'batch' job.
An easy way to run a batch job and downloading the result is::

    evi_composite.execute_batch("evi_composite.tiff", out_format="GTiff")

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
using :py:func:`~openeo.rest.connection.DataCube.polygonal_mean_timeseries`
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

.. image:: _static/images/timeseries.png
  :width: 400
  :alt: plotted timeseries

The same method also works for multiple polygons, or GeoJSON or SHP files that are
accessible by the back-end. This allows computing aggregated values over very large areas.

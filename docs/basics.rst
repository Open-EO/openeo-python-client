================
Getting Started
================


Connect to an openEO back-end
==============================

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
=====================

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

.. tip::
    The openEO Python client library comes with **Jupyter (notebook) integration** in a couple of places.
    For example, put ``connection.describe_collection("SENTINEL2_L2A")`` (without ``print()``)
    as last statement in a notebook cell
    and you'll get a nice graphical rendering of the collection metadata.

.. seealso::

    Find out more about data discovery, loading and filtering at :ref:`data_access_chapter`.


Authentication
==============

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
and it will print a web link and a short *user code*, for example:

.. code-block::

    To authenticate: visit https://aai.egi.eu/oidc/device and enter the user code 'Ka4rJ4L'.

Visit this web page in a browser, log in there with an existing account and enter the user code.
If everything goes well, the ``connection`` object in the script will be authenticated
and the back-end will be able to identify you in subsequent requests.



Example use case: EVI timeseries
==================================

A common task in earth observation is to apply a formula to a number of spectral bands
in order to compute an 'index', such as NDVI, NDWI, EVI, ...
In this tutorial we'll go through a couple of steps to extract a timeseries
of EVI values (enhanced vegetation index) for a certain region
and discuss some openEO concepts along the way.


Loading an initial data cube
=============================

For calculating the EVI, we need the reflectance of the
red, blue and (near) infrared spectral components.
These spectral bands are part of the well-known Sentinel-2 data set
and is available on the current back-end under collection id ``SENTINEL2_L2A``.
We load an initial small spatio-temporal slice (a data cube) as follows:

.. code-block:: python

    sentinel2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={"west": 5.15, "south": 51.181, "east": 5.155, "north": 51.184},
        temporal_extent=["2016-01-01", "2016-03-10"],
        bands=["B02", "B04", "B08"]
    )

Note how we specify a the region of interest, a time range and a set of bands to load.

.. note::
    By filtering as early as possible (directly in :py:meth:`~openeo.rest.connection.Connection.load_collection` in this case),
    we make sure the back-end only loads the data we are interested in
    and avoid incurring unneeded costs.

The :py:meth:`~openeo.rest.connection.Connection.load_collection` method on the connection
object created a :py:class:`~openeo.rest.datacube.DataCube` object (variable ``sentinel2_cube``).

.. important::
    It is important to highlight that we *did not load any real data* yet,
    instead we just created an abstract *client-side reference*,
    encapsulating the collection id, the spatial extent, the temporal extent, etc.
    The actual data loading will only happen at the back-end
    once we explicitly trigger the execution of the data processing pipeline we are building.


Band math
=========

From this data cube, we can now select the individual bands
(with the :py:meth:`DataCube.band() <openeo.rest.datacube.DataCube>` method)
and rescale the digital number values to physical reflectances:

.. code-block:: python

    blue = sentinel2_cube.band("B02") * 0.0001
    red = sentinel2_cube.band("B04") * 0.0001
    nir = sentinel2_cube.band("B08") * 0.0001

We now want to compute the enhanced vegetation index
and can do that directly with these band variables:

.. code-block:: python

    evi_cube = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)

.. important::
    As noted before: while this looks like an actual calculation,
    there is *no real data processing going on here*.
    The ``evi_cube`` object at this point is just an abstract representation
    of our algorithm under construction.
    The mathematical operators we used here are *syntactic sugar*
    for expressing this part of the algorithm in a very compact way.

    As an illustration of this, let's have peek at the *JSON representation*
    of our algorithm so far, the so-called *openEO process graph*:

    .. code-block:: text

        >>> print(evi_cube.to_json(indent=None))
        {"process_graph": {"loadcollection1": {"process_id": "load_collection", ...
        ... "id": "SENTINEL2_L2A", "spatial_extent": {"west": 5.15, "south": ...
        ... "multiply1": { ... "y": 0.0001}}, ...
        ... "multiply3": { ... {"x": 2.5, "y": {"from_node": "subtract1"}}} ...
        ...

    Note how the ``load_collection`` arguments, rescaling and EVI calculation aspects
    can be deciphered from this.
    Rest assured, as user you normally you don't have to worry too much
    about these process graph details,
    the openEO Python Client library handles this behind the scenes for you.


Download (synchornously)
========================

Let's download this as a GeoTIFF file.
Because GeoTIFF does not support a temporal dimension,
we first eliminate it by taking the temporal maximum value for each pixel:

.. code-block:: python

    evi_composite = evi_cube.max_time()

.. note::

    This :py:meth:`~openeo.rest.datacube.DataCube.max_time()` is not an official openEO process
    but one of the many *convenience methods* in the openEO Python Client Library
    to simplify common processing patterns.
    It provides a ``reduce`` operation along the temporal dimension with a ``max`` reducer/aggregator.

Now we can download this to a local file:

.. code-block:: python

    evi_composite.download("evi_composite.tiff")

This download command **triggers the actual processing** on the back-end:
it sends the process graph to the back-end and waits for the result.
It is a *synchronous operation* (the :py:meth:`~openeo.rest.datacube.DataCube.download()` call
blocks until the result is fully downloaded) and because we work on a small spatio-temporal extent,
this should only take a couple of seconds.


Batch Jobs (asynchronous execution)
===================================

Synchronous downloads are handy for quick experimentation on small data cubes,
but if you start processing larger data cubes, you can easily
hit *computation time limits* or other constraints.
For these larger tasks, it is recommended to work with **batch jobs**,
which allow you to work asynchronously:
after you start your job, you can disconnect (stop your script or even close your computer)
and then minutes/hours later you can reconnect to check the batch job status and download results.
The openEO Python Client Library also provides helpers to keep track of a running batch job
and show a progress report.

.. seealso::

    See :ref:`batch-jobs-chapter` for more details.


Applying a cloud mask
=========================

It is very common for earth observation data to have separate masking layers that for instance indicate
whether a pixel is covered by a (type of) cloud or not. For Sentinel-2, one such layer is the 'scene classification'
layer generated by the Sen2Cor algorithm.
In this example, we will use this layer to mask clouds out of our data.

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

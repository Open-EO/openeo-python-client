.. _data_access_chapter:


========================
Finding and loading data
========================

As illustrated in the basic concepts, most openEO scripts start with 'load_collection', but this skips the step of
actually finding out which collection to load. This section dives a bit deeper into finding the right data, and some more
advanced data loading use cases.

Data discovery
--------------

To explore data in a given back-end, it is recommended to use a more visual tool like the openEO Hub
(http://hub.openeo.org/). This shows available collections, and metadata in a user-friendly manner.

Next to that, the client also offers various :py:class:`~openeo.rest.connection.Connection` methods
to explore collections and their metadata:

- :py:meth:`~openeo.rest.connection.Connection.list_collection_ids`
  to list all collection ids provided by the back-end
- :py:meth:`~openeo.rest.connection.Connection.list_collections`
  to list the basic metadata of all collections
- :py:meth:`~openeo.rest.connection.Connection.describe_collection`
  to get the complete metadata of a particular collection

When using these methods inside a Jupyter notebook, you should notice that the output is rendered in a user friendly way.

In a regular script, these methods can be used to programmatically find a collection that matches specific criteria.

As a user, make sure to carefully read the documentation for a given collection, as there can be important differences.
You should also be aware of the data retention policy of a given collection: some data archives only retain the last 3 months
for instance, making them only suitable for specific types of analysis. Such differences can have an impact on the reproducibility
of your openEO scripts.

Also note that the openEO metadata may use links to point to much more information for a particular collection. For instance
technical specification on how the data was preprocessed, or viewers that allow you to visually explore the data. This can
drastically improve your understanding of the dataset.

Finally, licensing information is important to keep an eye on: not all data is free and open.

Loading collections
-------------------

Many examples already show the basic load_collection process, with filters on space, time and bands. In this section, we
want to dive a bit into details, and more advanced cases.


Exploring collections
#####################

A common question from users is about very specific details of a collection, we'd like to list some examples and solutions here:

- The collection data type, and range of values, can be determined by simply downloading a sample of data, as NetCDF or Geotiff. This can in fact be done at any point in the design of your script, to get a good idea of intermediate results.
- Data availability, and available timestamps can be retrieved by computing average values for your area of interest. Just construct a polygon, and retrieve those statistics. For optical data, this can also be used to get an idea on cloud statistics.
- Most collections have a native projection system, again a simple download will give you this information if its not clear from the metadata.

.. _data-reduction-section:

Data reduction
##############

The purpose of the filters in load_collection is to reduce the amount of data that is loaded by the back-end. We
recommend doing this, especially when experimenting with a script, as this can dramatically increase the response time for
your calls. Gradually increasing the amount of data processed is recommended when tests on smaller areas are successful.

Next to specifying filters inside load_collection, there are also possibilities to filter at a later stage in your process graph.
This can be very convenient, as you can avoid passing in all filtering parameters to the method that constructs a particular
datacube.

Another nice feature, is that processes that work on a vector feature, like aggregated statistics for a polygon, or masking
by polygon can also be used by a back-end to apply a spatial filter. This way, you do not need to explicitly set these
filters yourself.


Filtering on spatial extent
############################

A spatial extent is a bounding box that specifies the minimum and and maximum longitude and latitude of the region of interest you want to process.

By default these latitude and longitude values are expressed in the standard Coordinate Reference System for the world, which is EPSG:4623, also known as "WGS 84", or just "lat-long".

.. code-block:: python

    spatial_extent={"west": 5.14, "south": 51.17, "east": 5.17, "north": 51.19}


.. _filtering-on-temporal-extent-section:

Filtering on temporal extent
############################

Limiting the time range can greatly reduce the amount of data you have to process.
So it is best to select an appropriate time range that is not much larger than the data you really need.
And the same is true for limiting the spatial extent.

As you have seen in previous examples, the temporal extent is specified as a pair containing a start date and end date, which are expressed as strings in the format ``"yyyy-mm-dd"``.

.. code-block:: python

    connection.load_collection("SENTINEL2_L2A",
        spatial_extent={'west': 3.75, 'east': 4.08, 'south': 51.29, 'north': 51.39},
        temporal_extent=["2021-05-07", "2021-05-14"],
        bands=['B04', 'B03', 'B02'],
        max_cloud_cover=80,
    )

When you have more granular data, for example hourly measurements, you can also specify datetimes.
In that case we follow the RFC 3339 format to express datetime values.

For example:

.. code-block:: python

    # This means 2020-03-17 at 12:34:56 in UTC.
    "2020-03-17T12:34:56Z"


Start date included, end date excluded
######################################

Normally the time ranges in openEO are half-open intervals that are left-closed. In other words, the data from the start date will be included, but data from end date will be excluded from the data set.

Using half-open intervals makes sense for a time series because you don't want the start and end of each time slot to be counted twice, or have them overlap somehow.

Note however that we also have a shorthand notation that make it easier to specify an entire year or entire month, and that format deviates a bit from this rule, to make its use more convenient.


Tip: shorthand notation for temporal extent of a year or a months
#################################################################

.. note::

    Extent handling based on year/month is available since version 0.23.0.

For the temporal extent we allow a shorthand notation that allows you to select an entire year or an entire month without needing to specify a tuple with  the start date and end date.
In this case you just give it one string with the year, or the month.
The format for months is "yyyy-mm".

Examples or shorthand temporal extents:

.. code-block:: python

    # Process all data for the year 2021:
    sentinel2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={"west": 5.14, "south": 51.17, "east": 5.17, "north": 51.19},
        temporal_extent = "2021",
        bands=["B02", "B04", "B08"]
    )

.. code-block:: python

    # Process all data for the month of september in 2021:
    sentinel2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={"west": 5.14, "south": 51.17, "east": 5.17, "north": 51.19},
        temporal_extent = "2021-09",
        bands=["B02", "B04", "B08"]
    )

You can also specify a range of years or months, for example:

``temporal_extent = ["2021", "2023"]``

And this is in fact equivalent with:
``temporal_extent = ["2021-01_01", "2024-01-01"]``

Note that in the latter expression, the end date is **excluded**.
Therefor, 2024-01-01 is the first day that is no longer part of the time slot you want to process, and not 2023-12-31

.. code-block:: python

    # Process all data for the years 2021 up to, and including, 2023:
    sentinel2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={"west": 5.14, "south": 51.17, "east": 5.17, "north": 51.19},
        temporal_extent = ["2021", "2023"],
        bands=["B02", "B04", "B08"]
    )

.. note::

    This expression:
    ``temporal_extent = ["2021", "2023"]``

    is equivalent with this one:
    ``temporal_extent = ["2021-01_01", "2024-01-01"]``

    And note that in the later expression the end date is the first day that is no longer part of the data you want to process.

    Normally the end of the temporal extend is not included in the data,
    because the interval is left-closed to prevent overlaps.
    At least, that is the case when you are specifying days and datetimes in full.

    However, to make the use of ranges with years or months a little bit more natural,
    we treat this type of range as a *closed* interval instead,
    since that is what most people would expect in everyday language when we say things like
    "2016 to 2018" or "from march 2022 to june 2022".

    While that goes a bit against the normal convention, this makes it more convenient.

    Just keep in mind that when you are specifying a year or a month,
    that expression is really only an abbreviation of the real date range.
    It is not a "normal" specification where the date would be stated as a day for the start and the end.
    And since it already takes a shortcut, we might as well make its use as natural and convenient as possible.

Filtering on properties
#######################

Although openEO presents data in a data cube, a lot of collections are still backed by a product based catalog. This
allows filtering on properties of that catalog.

One example is filtering on the relative orbit number of SAR data. This example shows how that can be achieved::

    connection.load_collection(
        "SENTINEL1_GRD",
        spatial_extent={"west": 16.1, "east": 16.6, "north": 48.6, "south": 47.2},
        temporal_extent=["2018-01-01", "2019-01-01"],
        properties={
            "relativeOrbitNumber": lambda x: x==116
        }
    )

A similar and very useful example is to pre-filter Sentinel-2 products on cloud cover.
This avoids loading clouded data unnecessarily and increases performance.
:py:meth:`Connection.load_collection() <openeo.rest.connection.Connection.load_collection>` provides
a dedicated ``max_cloud_cover`` argument (shortcut for the ``eo:cloud_cover`` property) for that:

.. code-block:: python

    connection.load_collection("SENTINEL2_L2A",
        spatial_extent={'west': 3.75, 'east': 4.08, 'south': 51.29, 'north': 51.39},
        temporal_extent=["2021-05-07", "2021-05-14"],
        bands=['B04', 'B03', 'B02'],
        max_cloud_cover=80,
    )

Note that property names follow STAC metadata conventions, but some collections can have different names.

Property filters in openEO are also specified by small process graphs, that allow the use of the same generic processes
defined by openEO. This is the 'lambda' process that you see in the property dictionary. Do note that not all processes
make sense for product filtering, and can not always be properly translated into the query language of the catalog.
Hence, some experimentation may be needed to find a filter that works.

One important caveat in this example is that 'relativeOrbitNumber' is a catalog specific property name. Meaning that
different archives may choose a different name for a given property, and the properties that are available can depend
on the collection and the catalog that is used by it. This is not a problem caused by openEO, but by the limited
standardization between catalogs of EO data.


Handling large vector data sets
-------------------------------

For simple use cases, it is common to directly embed geometries (vector data) in your openEO process graph.
Unfortunately, with large vector data sets this leads to very large process graphs
and you might hit certain limits,
resulting in HTTP errors like ``413 Request Entity Too Large`` or ``413 Payload Too Large``.

This problem can be circumvented by first uploading your vector data to a file sharing service
(like Google Drive, DropBox, GitHub, ...)
and use its public URL in the process graph instead
through :py:meth:`Connection.vectorcube_from_paths <openeo.rest.connection.Connection.vectorcube_from_paths>`.
For example, as follows:

.. code-block:: python

    # Load vector data from URL
    url = "https://github.com/Open-EO/openeo-python-client/raw/master/tests/data/example_aoi.pq"
    parcels = connection.vectorcube_from_paths([url], format="parquet")

    # Use the parcel vector data, for example to do aggregation.
    cube = connection.load_collection(
        "SENTINEL2_L2A",
        bands=["B04", "B03", "B02"],
        temporal_extent=["2021-05-12", "2021-06-01"],
    )
    aggregations = cube.aggregate_spatial(
        geometries=parcels,
        reducer="mean",
    )

Note that while openEO back-ends typically support multiple vector formats, like GeoJSON and GeoParquet,
it is usually recommended to use a compact format like GeoParquet, instead of GeoJSON. The list of supported formats
is also advertised by the backend, and can be queried with
:py:meth:`Connection.list_file_formats <openeo.rest.connection.Connection.list_file_formats>`.

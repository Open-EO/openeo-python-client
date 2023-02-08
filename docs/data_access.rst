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
for instance, making them only suitable for specific types of analysis. Such differences can have an impact on the reproducability
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

Data reduction
##############

The purpose of the filters in load_collection is to reduce the amount of data that is loaded by the back-end. We
recommend doing this, especially when experimenting with a script, as this can dramatically increase the response time for
your calls. Gradually increasing the amount of data processed is recommended when tests on smaller areas are successfull.

Next to specifying filters inside load_collection, there are also possibilities to filter at a later stage in your process graph.
This can be very convenient, as you can avoid passing in all filtering parameters to the method that constructs a particular
datacube.

Another nice feature, is that processes that work on a vector feature, like aggregated statistics for a polygon, or masking
by polygon can also be used by a back-end to apply a spatial filter. This way, you do not need to explicitly set these
filters yourself.

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


Handling large vector data
##########################

Handling large volumes of data is crucial for making decisions, improving processes and having an efficient solution.
However, if you attempt to use a significantly large vector dataset directly on your process, you would most likely
encounter a 'Request Entity Too Large' error because your REST request was too large for the system to handle. In
order to avoid this error, you can upload your vector data to a public location(e.g., via Google Drive/Github) and use it as an URL. The data stored in the URL can be loaded using
:py:meth:`~openeo.rest.connection.Connection.vectorcube_from_paths`

The code-snippets shown below provides an example of how this can be achieved:

.. code-block:: python

    parcels = connection.vectorcube_from_paths(
      ["https://github.com/Open-EO/openeo-python-client/blob/master/tests/data/example_aoi.geoparquet"],
      format="parquet",
      )
    datacube = connection.load_collection(
                "SENTINEL2_L2A",
                bands=['B04', 'B03', 'B02'],
                temporal_extent = ["2021-05-12",'2021-06-01']
                )
    s2_cube = datacube.aggregate_spatial(
                          geometries=parcels,
                          reducer="mean"
                          )

Please note that though :py:meth:`~openeo.rest.connection.Connection.vectorcube_from_paths` supports GeoJSON and Parquet file format.
Yet, it is recommended to use the parquet format for large vector dataset in comparision to GeoJSON.

========================
Finding and loading data
========================

As illustrated in the basic concepts, most openEO scripts start with 'load_collection', but this skips the step of
actually finding out which collection to load. This section dives a bit deeper into finding the right data, and some more
advanced data loading use cases.

Data discovery
--------------

To explore data in a given backend, it is recommended to use a more visual tool like the openEO Hub
(http://hub.openeo.org/). This shows available collections, and metadata in a user-friendly manner.

Next to that, the client also offers various methods:

- :func:`~openeo.rest.connection.Connection.list_collection_ids` to list all collection ids
- :func:`~openeo.rest.connection.Connection.list_collections` to list all collection metadata
- :func:`~openeo.rest.connection.Connection.describe_collection` to show metadata for a particular collection

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

The purpose of the filters in load_collection is to reduce the amount of data that is loaded by the backend. We
recommend doing this, especially when experimenting with a script, as this can dramatically increase the response time for
your calls. Gradually increasing the amount of data processed is recommended when tests on smaller areas are successfull.

Next to specifying filters inside load_collection, there are also possibilities to filter at a later stage in your process graph.
This can be very convenient, as you can avoid passing in all filtering parameters to the method that constructs a particular
datacube.

Another nice feature, is that processes that work on a vector feature, like aggregated statistics for a polygon, or masking
by polygon can also be used by a backend to apply a spatial filter. This way, you do not need to explicitly set these
filters yourself.

Filtering on properties
#######################

Although openEO presents data in a data cube, a lot of collections are still backed by a product based catalog. This
allows filtering on properties of that catalog.

One example is filtering on the relative orbit number of SAR data. This example shows how that can be achieved::

    connection.load_collection(
        "S1_GRD",
        spatial_extent={"west": 16.1, "east": 16.6, "north": 48.6, "south": 47.2},
        temporal_extent=["2018-01-01", "2019-01-01"],
        properties={
            "relativeOrbitNumber": lambda x: eq(x=x, y=116)
        }
    )

Property filters in openEO are also specified by small process graphs, that allow the use of the same generic processes
defined by openEO. This is the 'lambda' process that you see in the property dictionary. Do note that not all processes
make sense for product filtering, and can not always be properly translated into the query language of the catalog.
Hence, some experimentation may be needed to find a filter that works.

One important caveat in this example is that 'relativeOrbitNumber' is a catalog specific property name. Meaning that
different archives may choose a different name for a given property, and the properties that are available can depend
on the collection and the catalog that is used by it. This is not a problem caused by openEO, but by the limited
standardization between catalogs of EO data.



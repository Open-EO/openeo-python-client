.. openeo documentation master file, created by
   sphinx-quickstart on Fri Oct  6 13:02:27 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Open-EO Client
==============


.. image:: https://img.shields.io/badge/Status-proof--of--concept-yellow.svg

Usage
==================================

.. automodule:: openeo

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Example: Simple band math
-------------------------
A common task in earth observation, is to apply a formula to a number of bands
in order to compute an 'index', such as NDVI, NDWI, EVI, ...

Begin by importing the openeo module::

    >>> import openeo

Now we need to connect to a backend::

    >>> connection = openeo.connect('http://openeo.vgt.vito.be/openeo')

Now, we have a :class:`Connection <openeo.Connection>` object called ``connection``. We can
This object is our entry point to the backend, and allows us to discover its capabilities.

Information about a backend is most easily found on the OpenEO HUB: http://hub.openeo.org/

Band math usually starts from a raster data cube, with multiple spectral bands available.
The backend used here has a Sentinel-2 collection: CGS_SENTINEL2_RADIOMETRY_V102_001

    >>> sentinel2_data_cube = connection.image("CGS_SENTINEL2_RADIOMETRY_V102_001")

Now we have a :class:`ImageCollection <openeo.ImageCollection>` object called ``sentinel2_data_cube``.
Creating this object does not yet load any data, but virtually it can contain quite a lot of data.
Therefore, we need to filter it before we can use it::

    >>> sentinel2_data_cube = sentinel2_data_cube.date_range_filter("2016-01-01","2016-03-10") \
                                       .bbox_filter(left=652000,right=672000,top=5161000,bottom=5181000,srs="EPSG:32632") \


On this data cube, we can now select the individual bands::

    >>> B02 = sentinel2_data_cube.band('B02')
        B04 = sentinel2_data_cube.band('B04')
        B08 = sentinel2_data_cube.band('B08')

In this example, we'll compute the enhanced vegetation index (EVI)::

    >>> evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
        evi_cube.download("out.geotiff", bbox="", time=s2_radio.dates['to'])


API
===

High level Interface
--------------------

The high-level interface tries to provide an opinionated, Pythonic, API
to interact with OpenEO backends. It's aim is to hide some of the details
of using a web service, so the user can produce concise and readable code.

Users that want to interact with OpenEO on a lower level, and have more control, can
use the lower level classes.

.. autofunction:: openeo.connect

.. automodule:: openeo.connection
   :members:

.. automodule:: openeo.imagecollection
   :members:

.. automodule:: openeo.job
   :members:


Authentication
--------------

.. automodule:: openeo.auth.auth
   :members:

.. automodule:: openeo.auth.auth_bearer
   :members:

.. automodule:: openeo.auth.auth_none

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

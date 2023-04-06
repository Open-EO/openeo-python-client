===============================
Client-side (local) processing
===============================

.. warning::
    This is a new experimental feature and API, subject to change.

Background
-----

The client-side processing functionality allows to test and use openEO with its processes locally, i.e. without any connection to an openEO back-end.
It relies on the projects `openeo-pg-parser-networkx <https://github.com/Open-EO/openeo-pg-parser-networkx>`__, which provides an openEO process graph parsing tool, and `openeo-processes-dask <https://github.com/Open-EO/openeo-processes-dask>`__, which provides an xArray and Dask implementation of most openEO processes. 

Installation
-----

**Please note:** this feature requires ``Python>=3.9`` and has been tested
with ``openeo-pg-parser-networkx==2023.3.1`` and
``openeo-processes-dask==2023.3.2``

.. code:: bash

   pip install openeo[localprocessing]

Usage
-----

Local Collections
~~~~~~~~

Every openEO process graph relies on data, which was always provided by a cloud infrastructure (the openEO back-end) until now.
The client-side processing adds the possibility to read and use local netCDFs, geoTIFFs and ZARR files for your experiments.

If you want to use our sample data, please clone this repository:

.. code:: bash

   git clone https://github.com/Open-EO/openeo-localprocessing-data.git
   
With some sample data we can now check the STAC metadata for the local files by doing:

.. code:: python

   from openeo.local import LocalConnection
   local_data_folders = ['./openeo-localprocessing-data/sample_netcdf','./openeo-localprocessing-data/sample_geotiff']
   local_conn = LocalConnection(local_data_folders)
   local_conn.list_collections()

This code will parse the metadata content of each netCDF, geoTIFF or ZARR file in the provided folders and return a JSON object containing the STAC representation of the metadata. If this code is run in a Jupyter Notebook, the metadata will be rendered nicely.


**Please note:** the code expects local files to have a similar structure to the sample files provided `here <https://github.com/Open-EO/openeo-localprocessing-data.git>`__. If the code can not handle you special netCDF, you can still modify the function that reads the metadata from it `here <https://github.com/Open-EO/openeo-python-client/blob/90c0505fae47c56746e49c91476be5147be6e1d0/openeo/local/collections.py#L19>`__ and the function that reads the data `here <https://github.com/Open-EO/openeo-python-client/blob/90c0505fae47c56746e49c91476be5147be6e1d0/openeo/local/processing.py#L26>`__.

Local Processing
~~~~~~~~

Let's start with the provided sample netCDF of Sentinel-2 data:

.. code:: python

   local_collection = 'openeo-localprocessing-data/sample_netcdf/S2_L2A_sample.nc'
   s2_datacube = local_conn.load_collection(local_collection)
   # Check if the data is loaded correctly
   print(s2_datacube.execute())

As you can see in the previous example, we are using a call to execute() which will execute locally the generated openEO process graph. In this case, the process graph consist only in a single load_collection, which performs lazy loading of the data. With this first step you can check if the data is being read correctly by openEO.

Looking at the metadata of this netCDF sample, we can see that it contains the bands B04, B03, B02, B08 and SCL. Additionally, we also see that it is composed by more than one element in time and that it covers the month of June 2022.

We can now do a simple processing for demo purposes, let's compute the median NDVI in time and visualize the result:

.. code:: python

   B04 = s2_datacube.band('B04')
   B08 = s2_datacube.band('B08')
   NDVI = (B08-B04)/(B08+B04)
   NDVI_median = NDVI.reduce_dimension(dimension='t',reducer='median')
   result_NDVI = NDVI_median.execute()
   result_NDVI.plot.imshow(cmap='Greens')

.. image:: ../_static/images/local/local_ndvi.jpg

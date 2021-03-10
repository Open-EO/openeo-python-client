.. _ard:

==============================
Analysis Ready Data generation
==============================

For certain use cases, the preprocessed data collections available in the openEO backends are not sufficient or simply not
available. For that case, openEO supports a few very common preprocessing scenario:

- Atmospheric correction of optical data
- SAR backscatter computation

These processes also offer a number of parameters to customize the processing. There's also variants with a default
parametrization that results in data that is compliant with CEOS CARD4L specifications https://ceos.org/ard/.

We should note that these operations can be computationally expensive, so certainly affect overall processing time and
cost of your final algorithm. Hence, make sure to make an informed decision when you decide to use these methods.

Atmospheric correction
----------------------

The `atmospheric correction <https://processes.openeo.org/draft/#atmospheric_correction>`_ process can apply a chosen
method on raw 'L1C' data. The supported methods and input datasets depend on the backend, because not every method is
validated or works on any dataset, and different backends try to offer a variety of options. This gives you as a user
more options to run and compare different methods, and select the most suitable one for your case.


To perform an `atmospheric correction <https://processes.openeo.org/draft/#atmospheric_correction>`_, the user has to
load an uncorrected L1C optical dataset. On the resulting datacube, the :func:`~openeo.rest.datacube.DataCube.atmospheric_correction`
method can be invoked. Note that it may not be possible to apply certain processes to the raw input data: preprocessing
algorithms can be tightly coupled with the raw data, making it hard or impossible for the backend to perform operations
in between loading and correcting the data.

The CARD4L variant of this process is: :func:`~openeo.rest.datacube.DataCube.ard_surface_reflectance`. This process follows
CEOS specifications, and thus can additional processing steps, like a BRDF correction, that are not yet available as a
separate process.

Reference implementations
#########################

This section shows a few working examples for these processes.

EODC backend
************

EODC (https://openeo.eodc.eu/v1.0) supports ard_surface_reflectance, based on the FORCE toolbox. (https://github.com/davidfrantz/force)

Geotrellis backend
******************

The geotrellis backend (https://openeo.vito.be) supports :func:`~openeo.rest.datacube.DataCube.atmospheric_correction` with iCor and SMAC as methods.
The version of iCor only offers basic atmoshperic correction features, without special options for water products: https://remotesensing.vito.be/case/icor
SMAC is implemented based on: https://github.com/olivierhagolle/SMAC
Both methods have been tested with Sentinel-2 as input. The viewing and sun angles need to be selected by the user to make them
available for the algorithm.

This is an example of applying iCor::

    l1c = connection.load_collection("SENTINEL2_L1C_SENTINELHUB",
            spatial_extent={'west':3.758216409030558,'east':4.087806252,'south':51.291835566,'north':51.3927399,'crs':'EPSG:4326'},
            temporal_extent=["2017-03-07","2017-03-07"],bands=['B04','B03','B02','B09','B8A','B11','sunAzimuthAngles','sunZenithAngles','viewAzimuthMean','viewZenithMean'] )
    l1c.atmospheric_correction(method="iCor").download("rgb-icor.geotiff",format="GTiff")


SAR backscatter
---------------

Data from synthetic aperture radar sensors requires significant preprocessing to be calibrated and normalized for terrain.
This is referred to as backscatter computation, and supported by
`sar_backscatter <https://processes.openeo.org/draft/#sar_backscatter>`_ and the CARD4L compliant variant
`ard_normalized_radar_backscatter <https://processes.openeo.org/draft/#ard_normalized_radar_backscatter>`_

The user should load a datacube containing raw SAR data, such as Sentinel-1 GRD. On the resulting datacube, the
:func:`~openeo.rest.datacube.DataCube.sar_backscatter` method can be invoked. The CEOS CARD4L variant is:
:func:`~openeo.rest.datacube.DataCube.ard_normalized_radar_backscatter`. These processes are tightly coupled to
metadata from specific sensors, so it is not possible to apply other processes to the datacube first,
with the exception of specifying filters in space and time.


Reference implementations
#########################

This section shows a few working examples for these processes.

EODC backend
************

EODC (https://openeo.eodc.eu/v1.0) supports sar_backscatter, based on the Sentinel-1 toolbox. (https://sentinel.esa.int/web/sentinel/toolboxes/sentinel-1)

Geotrellis backend
******************

When working with the Sentinelhub SENTINEL1_GRD collection, both sar processes can be used. The underlying implementation is
provided by Sentinelhub, (https://docs.sentinel-hub.com/api/latest/data/sentinel-1-grd/#processing-options), and offers full
CARD4L compliant processing options.

This is an example of :func:`~openeo.rest.datacube.DataCube.ard_normalized_radar_backscatter`::

    s1grd = (connection.load_collection('SENTINEL1_GRD', bands=['VH', 'VV'])
     .filter_bbox(west=2.59003, east=2.8949, north=51.2206, south=51.069, crs="EPSG:4326")
     .filter_temporal(extent=["2019-10-10","2019-10-10"]))

    job = s1grd.ard_normalized_radar_backscatter().execute_batch()

    for asset in job.get_results().get_assets():
        asset.download()

When working with other GRD data, an implementation based on Orfeo Toolbox is used:

- `Orfeo docs <https://www.orfeo-toolbox.org/CookBook/Applications/app_SARCalibration.html>`_
- `Implementation <https://github.com/Open-EO/openeo-geopyspark-driver/blob/master/openeogeotrellis/collections/s1backscatter_orfeo.py>`_

The Orfeo implementation currently only supports sigma0 computation, and is not CARD4L compliant.
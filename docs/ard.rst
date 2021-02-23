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

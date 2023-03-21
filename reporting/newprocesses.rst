openEO Python client library: new processes
=============================================

Listed below is the API documentation of the functions and methods
in the openEO Python client library
related to new processes that were added for each SRR.

SRR1
----


``DataCube`` methods
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.DataCube.ard_normalized_radar_backscatter
.. autofunction:: openeo.DataCube.ard_surface_reflectance
.. autofunction:: openeo.DataCube.atmospheric_correction
.. autofunction:: openeo.DataCube.sar_backscatter


Functions under ``openeo.processes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.processes.cloud_detection




.. raw:: latex

    \newpage

SRR2
----

``Connection`` methods
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.Connection.load_result


``DataCube`` methods
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.DataCube.fit_curve
.. autofunction:: openeo.DataCube.predict_curve
.. autofunction:: openeo.DataCube.resample_cube_temporal
.. autofunction:: openeo.DataCube.sar_backscatter
.. autofunction:: openeo.DataCube.aggregate_temporal_period


Functions under ``openeo.processes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.processes.array_concat
.. autofunction:: openeo.processes.array_create




.. raw:: latex

    \newpage

SRR3
----

``Connection`` methods
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.Connection.load_ml_model


``DataCube`` methods
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.DataCube.fit_class_random_forest
.. autofunction:: openeo.DataCube.fit_regr_random_forest
.. autofunction:: openeo.DataCube.predict_random_forest
.. autofunction:: openeo.DataCube.flatten_dimensions
.. autofunction:: openeo.DataCube.unflatten_dimension


``MlModel`` methods
~~~~~~~~~~~~~~~~~~~~~


.. autofunction:: openeo.rest.mlmodel.MlModel.save_ml_model


Functions under ``openeo.processes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: openeo.processes.vector_to_random_points
.. autofunction:: openeo.processes.vector_to_regular_points




.. raw:: latex

    \newpage

SRR4
----

Unlike the previous SRR iterations,
there was no need
in the openEO Python client library
for the addition of methods/functions/processes
related to specific SRR4 tasks.

Apart from general improvements and bug fixes,
here are a couple of notable changes
since SRR3 (version 0.10.0):

- Documentation improvements:

    - rework of "getting started" docs and landing page
    - rework of batch job docs
    - rework of UDF docs (some parts are still work in progress)
    - added overview page that maps openEO process to related openEO Python client library functions/methods.

- Simplify/improve workflow for using UDFs
  (loading from file, autodetect language, hide internals better)
- :py:class:`RESTJob` has been renamed to less cryptic and more user-friendly :py:class:`BatchJob`
- Align OpenID Connect handling with recent changes
  in EGI Check-In




.. raw:: latex

    \newpage

SRR5
----


Local processing
~~~~~~~~~~~~~~~~~


The most important new feature related to SRR5 specifically
is the basic implementation of the experimental "local processing" feature,
which allows end users to use openEO Python Client Library functionality
fully locally:

- loading local GeoTIFF/NetCDF files like "collections"
- do the processing locally using the ``openeo_processes_dask`` package
  (for example for debugging purposes or a faster development cycle).

API docs
^^^^^^^^^

.. autoclass:: openeo.local.connection.LocalConnection
    :members:


Various
~~~~~~~~

Furthermore, there were various other improvements
not specially tied to SRR5-specific tasks or use cases.
A couple of the notable changes since SRR4 (version 0.13.0):

- The ``openeo`` package can now also be installed directly through conda (using the ``conda-forge`` channel).
- Added :py:class:`MultiBackendJobManager` to help with use cases
  where one wants to schedule and track multiple jobs on multiple back-ends,
  based on implementation from the ``openeo-classification`` project.
- A :py:class:`DataCube` in a Jupyter notebook is now visualized as its process graph
  (instead of showing a cryptic textual representation).
- Support simplified OIDC device code flow
  (where user code is included in authentication URL, skipping a manual copy-paste step).
- Improved Windows support (e.g. related to private refresh token storage).
- Less verbose log printing on a failed batch job.
- Support the user-uploaded files part of the openEO API.
- Extend documentation to all functions/methods in ``openeo.processes`` submodule,
  which covers now all official (and most proposed/experimental) openEO processes.

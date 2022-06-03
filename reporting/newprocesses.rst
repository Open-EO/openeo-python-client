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



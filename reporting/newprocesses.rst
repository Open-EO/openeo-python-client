openEO Python client library: new processes
=============================================

Listed below is the API documentation of the functions, classes
and methods related to new processes that were added for each SRR.

SRR1
----


``DataCube`` methods
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: openeo.DataCube
    :noindex:
    :members: ard_normalized_radar_backscatter, ard_surface_reflectance, atmospheric_correction, sar_backscatter


Functions under ``openeo.processes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: openeo.processes
    :noindex:
    :members: cloud_detection


SRR2
----

``Connection`` methods
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: openeo.Connection
    :noindex:
    :members: load_result


``DataCube`` methods
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: openeo.DataCube
    :noindex:
    :members: fit_curve, predict_curve, resample_cube_temporal, sar_backscatter, aggregate_temporal_period


Functions under ``openeo.processes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: openeo.processes
    :noindex:
    :members: array_concat, array_create

SRR3
----

``Connection`` methods
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: openeo.Connection
    :noindex:
    :members: load_ml_model


``DataCube`` methods
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: openeo.DataCube
    :noindex:
    :members: fit_class_random_forest, fit_regr_random_forest, flatten_dimensions, predict_random_forest, unflatten_dimension


``MlModel`` methods
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: openeo.rest.mlmodel.MlModel
    :noindex:
    :members: save_ml_model


Functions under ``openeo.processes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: openeo.processes
    :noindex:
    :members: vector_to_random_points, vector_to_regular_points

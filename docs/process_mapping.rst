
.. 
    !Warning! This is an auto-generated file.
    Do not edit directly.
    Generated from: ['process_mapping.py']

.. _openeo_process_mapping:

openEO Process Mapping
#######################

The table below maps openEO processes to the corresponding
method or function in the openEO Python Client Library.

.. list-table:: 
    :header-rows: 1

    *   - openEO process
        - openEO Python Client Method

    *   - `add <https://processes.openeo.org/#add>`_
        - :py:meth:`DataCube.add() <openeo.rest.datacube.DataCube.add>`, :py:meth:`DataCube.__add__() <openeo.rest.datacube.DataCube.__add__>`, :py:meth:`DataCube.__radd__() <openeo.rest.datacube.DataCube.__radd__>`
    *   - `add_dimension <https://processes.openeo.org/#add_dimension>`_
        - :py:meth:`DataCube.add_dimension() <openeo.rest.datacube.DataCube.add_dimension>`
    *   - `aggregate_spatial <https://processes.openeo.org/#aggregate_spatial>`_
        - :py:meth:`DataCube.aggregate_spatial() <openeo.rest.datacube.DataCube.aggregate_spatial>`
    *   - `aggregate_temporal <https://processes.openeo.org/#aggregate_temporal>`_
        - :py:meth:`DataCube.aggregate_temporal() <openeo.rest.datacube.DataCube.aggregate_temporal>`
    *   - `aggregate_temporal_period <https://processes.openeo.org/#aggregate_temporal_period>`_
        - :py:meth:`DataCube.aggregate_temporal_period() <openeo.rest.datacube.DataCube.aggregate_temporal_period>`
    *   - `and <https://processes.openeo.org/#and>`_
        - :py:meth:`DataCube.logical_and() <openeo.rest.datacube.DataCube.logical_and>`, :py:meth:`DataCube.__and__() <openeo.rest.datacube.DataCube.__and__>`
    *   - `apply <https://processes.openeo.org/#apply>`_
        - :py:meth:`DataCube.apply() <openeo.rest.datacube.DataCube.apply>`
    *   - `apply_dimension <https://processes.openeo.org/#apply_dimension>`_
        - :py:meth:`DataCube.apply_dimension() <openeo.rest.datacube.DataCube.apply_dimension>`
    *   - `apply_kernel <https://processes.openeo.org/#apply_kernel>`_
        - :py:meth:`DataCube.apply_kernel() <openeo.rest.datacube.DataCube.apply_kernel>`
    *   - `apply_neighborhood <https://processes.openeo.org/#apply_neighborhood>`_
        - :py:meth:`DataCube.apply_neighborhood() <openeo.rest.datacube.DataCube.apply_neighborhood>`
    *   - `ard_normalized_radar_backscatter <https://processes.openeo.org/#ard_normalized_radar_backscatter>`_
        - :py:meth:`DataCube.ard_normalized_radar_backscatter() <openeo.rest.datacube.DataCube.ard_normalized_radar_backscatter>`
    *   - `ard_surface_reflectance <https://processes.openeo.org/#ard_surface_reflectance>`_
        - :py:meth:`DataCube.ard_surface_reflectance() <openeo.rest.datacube.DataCube.ard_surface_reflectance>`
    *   - `atmospheric_correction <https://processes.openeo.org/#atmospheric_correction>`_
        - :py:meth:`DataCube.atmospheric_correction() <openeo.rest.datacube.DataCube.atmospheric_correction>`
    *   - `count <https://processes.openeo.org/#count>`_
        - :py:meth:`DataCube.count_time() <openeo.rest.datacube.DataCube.count_time>`
    *   - `dimension_labels <https://processes.openeo.org/#dimension_labels>`_
        - :py:meth:`DataCube.dimension_labels() <openeo.rest.datacube.DataCube.dimension_labels>`
    *   - `divide <https://processes.openeo.org/#divide>`_
        - :py:meth:`DataCube.divide() <openeo.rest.datacube.DataCube.divide>`, :py:meth:`DataCube.__truediv__() <openeo.rest.datacube.DataCube.__truediv__>`
    *   - `drop_dimension <https://processes.openeo.org/#drop_dimension>`_
        - :py:meth:`DataCube.drop_dimension() <openeo.rest.datacube.DataCube.drop_dimension>`
    *   - `eq <https://processes.openeo.org/#eq>`_
        - :py:meth:`DataCube.__eq__() <openeo.rest.datacube.DataCube.__eq__>`
    *   - `filter_bands <https://processes.openeo.org/#filter_bands>`_
        - :py:meth:`DataCube.filter_bands() <openeo.rest.datacube.DataCube.filter_bands>`
    *   - `filter_bbox <https://processes.openeo.org/#filter_bbox>`_
        - :py:meth:`DataCube.filter_bbox() <openeo.rest.datacube.DataCube.filter_bbox>`
    *   - `filter_spatial <https://processes.openeo.org/#filter_spatial>`_
        - :py:meth:`DataCube.filter_spatial() <openeo.rest.datacube.DataCube.filter_spatial>`
    *   - `filter_temporal <https://processes.openeo.org/#filter_temporal>`_
        - :py:meth:`DataCube.filter_temporal() <openeo.rest.datacube.DataCube.filter_temporal>`
    *   - `fit_class_random_forest <https://processes.openeo.org/#fit_class_random_forest>`_
        - :py:meth:`DataCube.fit_class_random_forest() <openeo.rest.datacube.DataCube.fit_class_random_forest>`
    *   - `fit_curve <https://processes.openeo.org/#fit_curve>`_
        - :py:meth:`DataCube.fit_curve() <openeo.rest.datacube.DataCube.fit_curve>`
    *   - `fit_regr_random_forest <https://processes.openeo.org/#fit_regr_random_forest>`_
        - :py:meth:`DataCube.fit_regr_random_forest() <openeo.rest.datacube.DataCube.fit_regr_random_forest>`
    *   - `flatten_dimensions <https://processes.openeo.org/#flatten_dimensions>`_
        - :py:meth:`DataCube.flatten_dimensions() <openeo.rest.datacube.DataCube.flatten_dimensions>`
    *   - `ge <https://processes.openeo.org/#ge>`_
        - :py:meth:`DataCube.__ge__() <openeo.rest.datacube.DataCube.__ge__>`
    *   - `gt <https://processes.openeo.org/#gt>`_
        - :py:meth:`DataCube.__gt__() <openeo.rest.datacube.DataCube.__gt__>`
    *   - `le <https://processes.openeo.org/#le>`_
        - :py:meth:`DataCube.__le__() <openeo.rest.datacube.DataCube.__le__>`
    *   - `linear_scale_range <https://processes.openeo.org/#linear_scale_range>`_
        - :py:meth:`DataCube.linear_scale_range() <openeo.rest.datacube.DataCube.linear_scale_range>`
    *   - `ln <https://processes.openeo.org/#ln>`_
        - :py:meth:`DataCube.ln() <openeo.rest.datacube.DataCube.ln>`
    *   - `load_collection <https://processes.openeo.org/#load_collection>`_
        - :py:meth:`DataCube.load_collection() <openeo.rest.datacube.DataCube.load_collection>`
    *   - `load_ml_model <https://processes.openeo.org/#load_ml_model>`_
        - :py:meth:`MlModel.load_ml_model() <openeo.rest.mlmodel.MlModel.load_ml_model>`
    *   - `log <https://processes.openeo.org/#log>`_
        - :py:meth:`DataCube.logarithm() <openeo.rest.datacube.DataCube.logarithm>`, :py:meth:`DataCube.log2() <openeo.rest.datacube.DataCube.log2>`, :py:meth:`DataCube.log10() <openeo.rest.datacube.DataCube.log10>`
    *   - `lt <https://processes.openeo.org/#lt>`_
        - :py:meth:`DataCube.__lt__() <openeo.rest.datacube.DataCube.__lt__>`
    *   - `mask <https://processes.openeo.org/#mask>`_
        - :py:meth:`DataCube.mask() <openeo.rest.datacube.DataCube.mask>`
    *   - `mask_polygon <https://processes.openeo.org/#mask_polygon>`_
        - :py:meth:`DataCube.mask_polygon() <openeo.rest.datacube.DataCube.mask_polygon>`
    *   - `max <https://processes.openeo.org/#max>`_
        - :py:meth:`DataCube.max_time() <openeo.rest.datacube.DataCube.max_time>`
    *   - `mean <https://processes.openeo.org/#mean>`_
        - :py:meth:`DataCube.mean_time() <openeo.rest.datacube.DataCube.mean_time>`
    *   - `median <https://processes.openeo.org/#median>`_
        - :py:meth:`DataCube.median_time() <openeo.rest.datacube.DataCube.median_time>`
    *   - `merge_cubes <https://processes.openeo.org/#merge_cubes>`_
        - :py:meth:`DataCube.merge_cubes() <openeo.rest.datacube.DataCube.merge_cubes>`
    *   - `min <https://processes.openeo.org/#min>`_
        - :py:meth:`DataCube.min_time() <openeo.rest.datacube.DataCube.min_time>`
    *   - `multiply <https://processes.openeo.org/#multiply>`_
        - :py:meth:`DataCube.multiply() <openeo.rest.datacube.DataCube.multiply>`, :py:meth:`DataCube.__neg__() <openeo.rest.datacube.DataCube.__neg__>`, :py:meth:`DataCube.__mul__() <openeo.rest.datacube.DataCube.__mul__>`, :py:meth:`DataCube.__rmul__() <openeo.rest.datacube.DataCube.__rmul__>`
    *   - `ndvi <https://processes.openeo.org/#ndvi>`_
        - :py:meth:`DataCube.ndvi() <openeo.rest.datacube.DataCube.ndvi>`
    *   - `neq <https://processes.openeo.org/#neq>`_
        - :py:meth:`DataCube.__ne__() <openeo.rest.datacube.DataCube.__ne__>`
    *   - `normalized_difference <https://processes.openeo.org/#normalized_difference>`_
        - :py:meth:`DataCube.normalized_difference() <openeo.rest.datacube.DataCube.normalized_difference>`
    *   - `not <https://processes.openeo.org/#not>`_
        - :py:meth:`DataCube.__invert__() <openeo.rest.datacube.DataCube.__invert__>`
    *   - `or <https://processes.openeo.org/#or>`_
        - :py:meth:`DataCube.logical_or() <openeo.rest.datacube.DataCube.logical_or>`, :py:meth:`DataCube.__or__() <openeo.rest.datacube.DataCube.__or__>`
    *   - `power <https://processes.openeo.org/#power>`_
        - :py:meth:`DataCube.__rpow__() <openeo.rest.datacube.DataCube.__rpow__>`, :py:meth:`DataCube.__pow__() <openeo.rest.datacube.DataCube.__pow__>`, :py:meth:`DataCube.power() <openeo.rest.datacube.DataCube.power>`
    *   - `predict_curve <https://processes.openeo.org/#predict_curve>`_
        - :py:meth:`DataCube.predict_curve() <openeo.rest.datacube.DataCube.predict_curve>`
    *   - `predict_random_forest <https://processes.openeo.org/#predict_random_forest>`_
        - :py:meth:`DataCube.predict_random_forest() <openeo.rest.datacube.DataCube.predict_random_forest>`
    *   - `reduce_dimension <https://processes.openeo.org/#reduce_dimension>`_
        - :py:meth:`DataCube.reduce_dimension() <openeo.rest.datacube.DataCube.reduce_dimension>`, :py:meth:`DataCube.reduce_temporal_udf() <openeo.rest.datacube.DataCube.reduce_temporal_udf>`, :py:meth:`DataCube.reduce_temporal_simple() <openeo.rest.datacube.DataCube.reduce_temporal_simple>`
    *   - `rename_dimension <https://processes.openeo.org/#rename_dimension>`_
        - :py:meth:`DataCube.rename_dimension() <openeo.rest.datacube.DataCube.rename_dimension>`
    *   - `rename_labels <https://processes.openeo.org/#rename_labels>`_
        - :py:meth:`DataCube.rename_labels() <openeo.rest.datacube.DataCube.rename_labels>`
    *   - `resample_cube_temporal <https://processes.openeo.org/#resample_cube_temporal>`_
        - :py:meth:`DataCube.resample_cube_temporal() <openeo.rest.datacube.DataCube.resample_cube_temporal>`
    *   - `resample_spatial <https://processes.openeo.org/#resample_spatial>`_
        - :py:meth:`DataCube.resample_spatial() <openeo.rest.datacube.DataCube.resample_spatial>`
    *   - `resolution_merge <https://processes.openeo.org/#resolution_merge>`_
        - :py:meth:`DataCube.resolution_merge() <openeo.rest.datacube.DataCube.resolution_merge>`
    *   - `run_udf <https://processes.openeo.org/#run_udf>`_
        - :py:meth:`VectorCube.run_udf() <openeo.rest.vectorcube.VectorCube.run_udf>`
    *   - `sar_backscatter <https://processes.openeo.org/#sar_backscatter>`_
        - :py:meth:`DataCube.sar_backscatter() <openeo.rest.datacube.DataCube.sar_backscatter>`
    *   - `save_result <https://processes.openeo.org/#save_result>`_
        - :py:meth:`VectorCube.save_result() <openeo.rest.vectorcube.VectorCube.save_result>`, :py:meth:`DataCube.save_result() <openeo.rest.datacube.DataCube.save_result>`
    *   - `subtract <https://processes.openeo.org/#subtract>`_
        - :py:meth:`DataCube.subtract() <openeo.rest.datacube.DataCube.subtract>`, :py:meth:`DataCube.__sub__() <openeo.rest.datacube.DataCube.__sub__>`, :py:meth:`DataCube.__rsub__() <openeo.rest.datacube.DataCube.__rsub__>`
    *   - `unflatten_dimension <https://processes.openeo.org/#unflatten_dimension>`_
        - :py:meth:`DataCube.unflatten_dimension() <openeo.rest.datacube.DataCube.unflatten_dimension>`

:subscript:`(Table autogenerated on 2022-06-09)`

******************
Machine Learning
******************

.. warning::
    This API and documentation is experimental,
    under heavy development and subject to change.



Random Forest based Classification and Regression
===================================================

openEO defines a couple of processes for *random forest* based machine learning
for Earth Observation applications:

- ``fit_class_random_forest`` for training a random forest based classification model
- ``fit_regr_random_forest`` for training a random forest based regression model
- ``predict_random_forest`` for inference/prediction

The openEO Python Client library provides the necessary functionality to set up
and execute training and inference workflows.

Training
---------

Let's focus on training a classification model, where we try to predict
something like a land cover type or crop type based on predictors
we derive from EO data.
For example, assume we have a GeoJSON FeatureCollection
of sample points and a corresponding classification target value as follows::

    feature_collection = {"type": "FeatureCollection", "features": [
        {
            "type": "Feature",
            "properties": {"id": "#54345", "target": 3},
            "geometry": {"type": "Point", "coordinates": [3.4, 51.1]}
        },
        {
            "type": "Feature",
            "properties": {"id": "#8776", "target": 5},
            "geometry": {"type": "Point", "coordinates": [3.6, 51.2]}
        },
        ...


.. note::
    Confusingly, the concept "feature" has somewhat conflicting meanings
    for different audiences. GIS/EO people use "feature" to refer to the "rows"
    in this feature collection.
    For the machine learning community however, the properties (the "columns")
    are the features.
    To avoid confusion in this discussion we will avoid the term "feature"
    and instead use "sample point" for the former and "predictor" for the latter.


We first build a datacube of "predictor" bands::

    cube = connection.load_collection(
        "SENTINEL2",
        temporal_extent=[start, end],
        spatial_extent=bbox,
        bands=["B02", "B03", "B04"]
    )
    cube = cube.reduce_dimension(dimension="t", reducer="mean")

After reducing the temporal dimension, we use ``aggregate_spatial``
to sample the cube at the sample points and get a vector cube
where we have the temporal mean of the B02/B03/B04 bands as predictor values::

    predictors = cube.aggregate_spatial(feature_collection, reducer="mean")

We can now train a model, by providing the ::

    model = predictors.fit_class_random_forest(
        target=feature_collection,
        training=0.8
    )
    model = model.save_ml_model()

And execute it as a batch job::

    training_job = model.create_job()
    training_job.start_and_wait()


Inference
----------

When the batch job finishes successfully, the trained model can then be used
with the ``predict_random_forest`` process on the raster data cube
(or another with the same structure) to classify all the pixels.
The model can be specified in :py:meth:`~openeo.rest.datacube.DataCube.predict_random_forest`
in several ways, such as the job id of the training job::

    predicted = cube.predict_random_forest(
        model=training_job.job_id,
        dimension="bands"
    )

    predicted.download("predicted.GTiff")



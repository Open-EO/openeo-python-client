******************
Machine Learning
******************

.. warning::
    This API and documentation is experimental,
    under heavy development and subject to change.


.. versionadded:: 0.10.0


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
a class like a land cover type or crop type based on predictors
we derive from EO data.
For example, assume we have a GeoJSON FeatureCollection
of sample points and a corresponding classification target value as follows::

    feature_collection = {"type": "FeatureCollection", "features": [
        {
            "type": "Feature",
            "properties": {"id": "b3dw-wd23", "target": 3},
            "geometry": {"type": "Point", "coordinates": [3.4, 51.1]}
        },
        {
            "type": "Feature",
            "properties": {"id": "r8dh-3jkd", "target": 5},
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


We first build a datacube of "predictor" bands.
For simplicity, we will just use the raw B02/B03/B04 band values here
and use the temporal mean to eliminate the time dimension::

    cube = connection.load_collection(
        "SENTINEL2",
        temporal_extent=[start, end],
        spatial_extent=bbox,
        bands=["B02", "B03", "B04"]
    )
    cube = cube.reduce_dimension(dimension="t", reducer="mean")

We now use ``aggregate_spatial`` to sample this *raster data cube* at the sample points
and get a *vector cube* where we have the temporal mean of the B02/B03/B04 bands as predictor values::

    predictors = cube.aggregate_spatial(feature_collection, reducer="mean")

We can now train a *Random Forest* model by calling the
:py:meth:`~openeo.rest.vectorcube.VectorCube.fit_class_random_forest` method on the predictor vector cube
and passing the original target class data::

    model = predictors.fit_class_random_forest(
        target=feature_collection,
    )
    # Save the model as a batch job result asset
    # so that we can load it in another job.
    model = model.save_ml_model()

Finally execute this whole training flow as a batch job::

    training_job = model.create_job()
    training_job.start_and_wait()

Inference
----------

When the batch job finishes successfully, the trained model can then be used
with the ``predict_random_forest`` process on the raster data cube
(or another cube with the same band structure) to classify all the pixels.

We inspect the result metadata of the training job to obtain the STAC Item URL of the trained model::


    results = training_job.get_results()
    links = results.get_metadata()['links']
    ml_model_metadata_url = [link for link in links if 'ml_model_metadata.json' in link['href']][0]['href']
    print(ml_model_metadata_url)


Next, load the model from the URL::

    model = connection.load_ml_model(id=ml_model_metadata_url)

Technically, the openEO ``predict_random_forest`` process has to be used as a reducer function
inside a ``reduce_dimension`` call, but the openEO Python client library makes it
a bit easier by providing a :py:meth:`~openeo.rest.datacube.DataCube.predict_random_forest` method
directly on the :py:class:`~openeo.rest.datacube.DataCube` class, so that you can just do::

    predicted = cube.predict_random_forest(
        model=model,
        dimension="bands"
    )

    predicted.download("predicted.GTiff")

We specified the model here by URL corresponding to the
STAC Item that implements the ml-model extension,
but it can also be specified in other ways:
as :py:class:`~openeo.rest.job.BatchJob` instance,
as job_id of training job (string),
or as :py:class:`~openeo.rest.mlmodel.MlModel` instance (e.g. loaded through
:py:meth:`~openeo.rest.connection.Connection.load_ml_model`).
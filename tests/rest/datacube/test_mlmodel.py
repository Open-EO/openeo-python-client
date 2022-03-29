import pytest

from openeo import RESTJob
from openeo.rest.mlmodel import MlModel
from .conftest import API_URL

FEATURE_COLLECTION_1 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"target": 3},
            "geometry": {"type": "Polygon", "coordinates": [[[3, 5], [4, 5], [4, 6], [3, 6], [3, 5]]]}
        },
        {
            "type": "Feature",
            "properties": {"target": 5},
            "geometry": {"type": "Polygon", "coordinates": [[[8, 1], [9, 1], [9, 2], [8, 2], [8, 1]]]}
        },

    ]
}


def test_fit_class_random_forest_basic(con100):
    geometries = FEATURE_COLLECTION_1
    s2 = con100.load_collection("S2")
    predictors = s2.aggregate_spatial(geometries, reducer="mean")
    ml_model = predictors.fit_class_random_forest(target=geometries)
    assert ml_model.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "aggregatespatial1": {
            "process_id": "aggregate_spatial",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "geometries": geometries,
                "reducer": {"process_graph": {
                    "mean1": {
                        "process_id": "mean",
                        "arguments": {"data": {"from_parameter": "data"}},
                        "result": True
                    }
                }}
            },
        },
        "fitclassrandomforest1": {
            "process_id": "fit_class_random_forest",
            "arguments": {
                "predictors": {"from_node": "aggregatespatial1"},
                "target": geometries,
                "num_trees": 100,
            },
            "result": True,
        }
    }


@pytest.mark.parametrize("explicit_save", [True, False])
def test_fit_class_random_forest_basic_create_job(con100, requests_mock, explicit_save, caplog):
    geometries = FEATURE_COLLECTION_1
    s2 = con100.load_collection("S2")
    predictors = s2.aggregate_spatial(geometries, reducer="mean")
    ml_model = predictors.fit_class_random_forest(target=geometries)
    if explicit_save:
        ml_model = ml_model.save_ml_model()

    def post_jobs(request, context):
        pg = request.json()["process"]["process_graph"]
        assert set(p["process_id"] for p in pg.values()) == {
            "load_collection", "aggregate_spatial",
            "fit_class_random_forest", "save_ml_model",
        }
        context.status_code = 201
        context.headers["OpenEO-Identifier"] = "job-rf"

    requests_mock.post(API_URL + "/jobs", json=post_jobs)

    job = ml_model.create_job(title="Random forest")
    assert job.job_id == "job-rf"
    assert ("no final `save_ml_model`. Adding it" in caplog.text) == (not explicit_save)


@pytest.mark.parametrize("id", [
    "https://oeo.test/my/model",
    "bAtch-j08-2dfe34-sfsd",
    "models/ni.model",
])
def test_load_ml_model_basic(con100, id):
    ml_model = con100.load_ml_model(id)
    assert isinstance(ml_model, MlModel)
    assert ml_model.flat_graph() == {
        "loadmlmodel1": {
            "process_id": "load_ml_model",
            "arguments": {"id": id},
            "result": True
        }
    }


def test_load_ml_model_from_job(con100):
    job = RESTJob(job_id="my-j08", connection=con100)
    ml_model = con100.load_ml_model(id=job)
    assert isinstance(ml_model, MlModel)
    assert ml_model.flat_graph() == {
        "loadmlmodel1": {
            "process_id": "load_ml_model",
            "arguments": {"id": "my-j08"},
            "result": True
        }
    }


@pytest.mark.parametrize("model_factory", [
    (lambda con100: con100.load_ml_model("my-j08")),
    (lambda con100: "my-j08"),
    (lambda con100: RESTJob("my-j08", con100)),
])
def test_predict_random_forest(con100, model_factory):
    ml_model = model_factory(con100)
    cube = con100.load_collection("S2")
    cube = cube.predict_random_forest(model=ml_model, dimension="bands")
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "loadmlmodel1": {
            "process_id": "load_ml_model",
            "arguments": {"id": "my-j08"},
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "reducer": {"process_graph": {
                    "predictrandomforest1": {
                        "process_id": "predict_random_forest",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "model": {"from_parameter": "context"}
                        },
                        "result": True,
                    }
                }},
                "dimension": "bands",
                "context": {"from_node": "loadmlmodel1"},
            },
            "result": True,
        }
    }

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
    ml_model = predictors.fit_class_random_forest(target=geometries, training=0.5)
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
                "training": 0.5,
                "num_trees": 100,
            },
            "result": True,
        }
    }


def test_fit_class_random_forest_basic_create_job(con100, requests_mock):
    geometries = FEATURE_COLLECTION_1
    s2 = con100.load_collection("S2")
    predictors = s2.aggregate_spatial(geometries, reducer="mean")
    ml_model = predictors.fit_class_random_forest(target=geometries, training=0.5)
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

    job = ml_model.send_job(title="Random forest")
    assert job.job_id == "job-rf"

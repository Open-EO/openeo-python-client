from openeo.extra.stac_job_db import STACAPIJobDatabase


def test_create_db():
    #TODO mock stac client
    db = STACAPIJobDatabase("biopar_jobs","https://stac.openeo.vito.be")

    assert db.exists()

    jobs = db.get_by_status(["not_started"])


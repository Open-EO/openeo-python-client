import re

import pytest

from openeo.rest import OpenEoApiError
from openeo.rest._testing import DummyBackend


@pytest.fixture
def dummy_backend120(requests_mock, con120):
    return DummyBackend(requests_mock=requests_mock, connection=con120)

@pytest.fixture
def dummy_backend130(requests_mock, con130):
    return DummyBackend(requests_mock=requests_mock, connection=con130)

DUMMY_PG_ADD35 = {
    "add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
}


class TestDummyBackend:
    def test_create_job(self, dummy_backend120, con120):
        assert dummy_backend120.batch_jobs == {}
        _ = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend120.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {"add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}},
                "status": "created",
            }
        }

    def test_start_job(self, dummy_backend120, con120):
        job = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend120.batch_jobs == {
            "job-000": {"job_id": "job-000", "pg": DUMMY_PG_ADD35, "status": "created"},
        }
        job.start()
        assert dummy_backend120.batch_jobs == {
            "job-000": {"job_id": "job-000", "pg": DUMMY_PG_ADD35, "status": "finished"},
        }

    def test_job_status_updater_error(self, dummy_backend120, con120):
        dummy_backend120.job_status_updater = lambda job_id, current_status: "error"

        job = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend120.batch_jobs["job-000"]["status"] == "created"
        job.start()
        assert dummy_backend120.batch_jobs["job-000"]["status"] == "error"

    @pytest.mark.parametrize("final", ["finished", "error"])
    def test_setup_simple_job_status_flow(self, dummy_backend120, con120, final):
        dummy_backend120.setup_simple_job_status_flow(queued=2, running=3, final=final)
        job = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend120.batch_jobs["job-000"]["status"] == "created"

        # Note that first status update (to "queued" here) is triggered from `start()`, not `status()` like below
        job.start()
        assert dummy_backend120.batch_jobs["job-000"]["status"] == "queued"

        # Now go through rest of status flow, through `status()` calls
        assert job.status() == "queued"
        assert job.status() == "running"
        assert job.status() == "running"
        assert job.status() == "running"
        assert job.status() == final
        assert job.status() == final
        assert job.status() == final
        assert job.status() == final

    def test_setup_simple_job_status_flow_final_per_job(self, dummy_backend120, con120):
        """Test per-job specific final status"""
        dummy_backend120.setup_simple_job_status_flow(
            queued=2, running=3, final="finished", final_per_job={"job-001": "error"}
        )
        job0 = con120.create_job(DUMMY_PG_ADD35)
        job1 = con120.create_job(DUMMY_PG_ADD35)
        job2 = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend120.batch_jobs["job-000"]["status"] == "created"
        assert dummy_backend120.batch_jobs["job-001"]["status"] == "created"
        assert dummy_backend120.batch_jobs["job-002"]["status"] == "created"

        # Note that first status update (to "queued" here) is triggered from `start()`, not `status()` like below
        job0.start()
        job1.start()
        job2.start()
        assert dummy_backend120.batch_jobs["job-000"]["status"] == "queued"
        assert dummy_backend120.batch_jobs["job-001"]["status"] == "queued"
        assert dummy_backend120.batch_jobs["job-002"]["status"] == "queued"

        # Now go through rest of status flow, through `status()` calls
        for expected_status in ["queued", "running", "running", "running"]:
            assert job0.status() == expected_status
            assert job1.status() == expected_status
            assert job2.status() == expected_status

        # Differentiation in final state
        for _ in range(3):
            assert job0.status() == "finished"
            assert job1.status() == "error"
            assert job2.status() == "finished"

    def test_setup_job_start_failure(self, dummy_backend120):
        job = dummy_backend120.connection.create_job(process_graph={})
        dummy_backend120.setup_job_start_failure()
        with pytest.raises(OpenEoApiError, match=re.escape("[500] Internal: No job starting for you, buddy")):
            job.start()
        assert job.status() == "error"

    def test_version(self, dummy_backend120, dummy_backend130):
        capabilities120 = dummy_backend120.connection.capabilities()
        capabilities130 = dummy_backend130.connection.capabilities()

        assert capabilities120.api_version() == "1.2.0"
        assert capabilities130.api_version() == "1.3.0"

    def test_jwt_conformance(self, dummy_backend120, dummy_backend130):
        capabilities120 = dummy_backend120.connection.capabilities()
        capabilities130 = dummy_backend130.connection.capabilities()

        assert capabilities120.has_conformance("https://api.openeo.org/*/authentication/jwt") == False
        assert capabilities130.has_conformance("https://api.openeo.org/*/authentication/jwt") == True
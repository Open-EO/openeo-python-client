import pytest

from openeo.rest._testing import DummyBackend


@pytest.fixture
def dummy_backend(requests_mock, con120):
    return DummyBackend(requests_mock=requests_mock, connection=con120)


DUMMY_PG_ADD35 = {
    "add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
}


class TestDummyBackend:
    def test_create_job(self, dummy_backend, con120):
        assert dummy_backend.batch_jobs == {}
        _ = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {"add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}},
                "status": "created",
            }
        }

    def test_start_job(self, dummy_backend, con120):
        job = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend.batch_jobs == {
            "job-000": {"job_id": "job-000", "pg": DUMMY_PG_ADD35, "status": "created"},
        }
        job.start()
        assert dummy_backend.batch_jobs == {
            "job-000": {"job_id": "job-000", "pg": DUMMY_PG_ADD35, "status": "finished"},
        }

    def test_job_status_updater_error(self, dummy_backend, con120):
        dummy_backend.job_status_updater = lambda job_id, current_status: "error"

        job = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend.batch_jobs["job-000"]["status"] == "created"
        job.start()
        assert dummy_backend.batch_jobs["job-000"]["status"] == "error"

    @pytest.mark.parametrize("final", ["finished", "error"])
    def test_setup_simple_job_status_flow(self, dummy_backend, con120, final):
        dummy_backend.setup_simple_job_status_flow(queued=2, running=3, final=final)
        job = con120.create_job(DUMMY_PG_ADD35)
        assert dummy_backend.batch_jobs["job-000"]["status"] == "created"

        # Note that first status update (to queued here) is triggered from `start()`, not `status()` like below
        job.start()
        assert dummy_backend.batch_jobs["job-000"]["status"] == "queued"

        # Now go through rest of status flow, through `status()` calls
        assert job.status() == "queued"
        assert job.status() == "running"
        assert job.status() == "running"
        assert job.status() == "running"
        assert job.status() == final
        assert job.status() == final
        assert job.status() == final
        assert job.status() == final

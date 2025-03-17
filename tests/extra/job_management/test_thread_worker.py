import logging
import time
from collections import defaultdict

import pytest

from openeo.extra.job_management.thread_worker import _JobManagerWorkerThreadPool


# Fixture for creating and cleaning up the worker thread pool
@pytest.fixture
def worker_pool():
    worker_pool = _JobManagerWorkerThreadPool()
    yield worker_pool
    worker_pool.shutdown()


# Fixture for the shared stats dictionary
@pytest.fixture
def stats():
    return defaultdict(int)


class TestJobManagerWorkerThreadPool:
    def test_worker_thread_lifecycle(self, worker_pool, caplog):
        with caplog.at_level(logging.INFO):
            assert not worker_pool._executor._shutdown
            worker_pool.shutdown()
            assert worker_pool._executor._shutdown
        assert "Shutting down worker thread pool" in caplog.text

    def test_process_futures_no_completed_futures(self, worker_pool, stats, caplog):
        with caplog.at_level(logging.INFO):
            # Submit work without any backend response mock
            worker_pool.submit_work(worker_pool.WORK_TYPE_START_JOB, ("https://foo.test", "token", "job-123"))
            # Process futures immediately; none should be complete
            worker_pool.process_futures(stats)
        assert stats["job start"] == 0  # No job marked as started
        assert "Processing 1 futures" in caplog.text
        assert "Processed 0 jobs" in caplog.text

    def test_start_job_success(self, worker_pool, stats, requests_mock):
        backend_url = "https://foo.test"
        job_id = "job-123"
        # Set up mocks for a successful job start
        requests_mock.get(backend_url, json={"api_version": "1.1.0"})
        requests_mock.post(
            f"{backend_url}/jobs",
            json={"job_id": job_id, "status": "created"},
            status_code=201,
            headers={"openeo-identifier": job_id},
        )
        requests_mock.post(
            f"{backend_url}/jobs/{job_id}/results", json={"job_id": job_id, "status": "finished"}, status_code=202
        )
        requests_mock.get(f"{backend_url}/jobs/{job_id}", json={"id": job_id, "status": "finished"})
        # Submit several valid jobs
        for _ in range(3):
            worker_pool.submit_work(worker_pool.WORK_TYPE_START_JOB, (backend_url, "token", job_id))
        time.sleep(1)
        worker_pool.process_futures(stats)
        assert stats["job start"] == 3
        assert len(worker_pool._futures) == 0

    def test_start_job_failure(self, worker_pool, stats, requests_mock, caplog):
        backend_url = "https://down.test"
        job_id = "job-123"
        # Simulate a connection error for the backend
        requests_mock.get(backend_url, exc=ConnectionError("Backend unreachable"))
        worker_pool.submit_work(worker_pool.WORK_TYPE_START_JOB, (backend_url, "token", job_id))
        time.sleep(1)
        worker_pool.process_futures(stats)
        assert stats["job start failed"] == 1
        assert f"Job {job_id} failed: Backend unreachable" in caplog.text

    def test_process_futures_mixed_success_and_failure(self, worker_pool, stats, requests_mock, caplog):
        # Successful job
        backend_url_success = "https://success.test"
        job_id_success = "job-success"
        requests_mock.get(backend_url_success, json={"api_version": "1.1.0"})
        requests_mock.post(
            f"{backend_url_success}/jobs/{job_id_success}/results",
            json={"job_id": job_id_success, "status": "finished"},
            status_code=202,
        )
        requests_mock.get(
            f"{backend_url_success}/jobs/{job_id_success}", json={"id": job_id_success, "status": "finished"}
        )
        # Failed job
        backend_url_failure = "https://failure.test"
        job_id_failure = "job-failure"
        requests_mock.get(backend_url_failure, exc=ConnectionError("Backend unreachable"))
        # Submit both jobs
        worker_pool.submit_work(worker_pool.WORK_TYPE_START_JOB, (backend_url_success, "token", job_id_success))
        worker_pool.submit_work(worker_pool.WORK_TYPE_START_JOB, (backend_url_failure, "token", job_id_failure))
        time.sleep(1)
        with caplog.at_level(logging.INFO):
            worker_pool.process_futures(stats)
        assert stats["job start"] == 1  # One successful job
        assert stats["job start failed"] == 1  # One failed job

    def test_invalid_work_type(self, worker_pool, stats, requests_mock, caplog):
        backend_url = "https://foo.test"
        job_id = "job-123"
        requests_mock.get(backend_url, json={"api_version": "1.1.0"})

        # Test that invalid work type raises ValueError
        with pytest.raises(ValueError) as exc_info:
            worker_pool.submit_work("invalid_work_type", (backend_url, "token", job_id))

        assert "Unsupported work type: invalid_work_type" in str(exc_info.value)
        assert len(worker_pool._futures) == 0

    @pytest.mark.parametrize(
        "work_args,expected_log",
        [
            (("https://foo.test", "token"), "Expected 3 arguments for work type start_job, got 2"),
            ((None, "token", "job-123"), "root_url must be a string"),
        ],
    )
    def test_invalid_work_args(self, worker_pool, stats, caplog, work_args, expected_log):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(Exception):  # Expect an exception to be raised
                worker_pool.submit_work(worker_pool.WORK_TYPE_START_JOB, work_args)

        # Verify the expected log message
        assert expected_log in caplog.text
        assert len(worker_pool._futures) == 0
        assert stats["job start"] == 0

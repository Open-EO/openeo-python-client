import time
import pytest
import pandas as pd
import requests

# Import the refactored classes and helper functions from your codebase.
# Adjust the import paths as needed.
from openeo.extra.job_management._thread_worker import (
    _JobManagerWorkerThreadPool,
    _JobStartTask)

# --- Fixtures and Helpers ---

@pytest.fixture
def worker_pool():
    """Fixture for creating and cleaning up a worker thread pool."""
    pool = _JobManagerWorkerThreadPool(max_workers=2)
    yield pool
    pool.shutdown()

@pytest.fixture
def sample_dataframe():
    """Creates a pandas DataFrame for job tracking."""
    df = pd.DataFrame([
        {"id": "job-123", "status": "queued_for_start", "other_field": "foo"},
        {"id": "job-456", "status": "queued_for_start", "other_field": "bar"},
        {"id": "job-789", "status": "other", "other_field": "baz"}
    ])
    return df

@pytest.fixture
def initial_stats():
    """Returns a dictionary with initial stats counters."""
    return {"job start": 0, "job start failed": 0}

@pytest.fixture
def successful_backend_mock(requests_mock):
    """
    Returns a helper to set up a successful backend.
    Mocks a version check, job start, and job status check.
    """
    def _setup(root_url: str, job_id: str, status: str = "queued"):
        # Backend version check
        requests_mock.get(root_url, json={"api_version": "1.1.0"})
        # Job start: assume that the job start endpoint returns a JSON response (simulate the backend behavior)
        requests_mock.post(f"{root_url}/jobs/{job_id}/results", json={"job_id": job_id, "status": status}, status_code=202)
        # Job status check
        requests_mock.get(f"{root_url}/jobs/{job_id}", json={"job_id": job_id, "status": status})
    return _setup

@pytest.fixture
def valid_task():
    """Fixture to create a valid _JobStartTask instance."""
    return _JobStartTask(
        root_url="https://foo.test",
        bearer_token="test-token",
        job_id="test-job-123"
    )

import time

def wait_for_results(worker_pool, timeout=3.0, interval=0.1):
    """
    Wait for the worker pool to return results, with timeout safety.
    Raises:
        TimeoutError if no results are available within timeout.
    """
    start = time.time()
    while time.time() - start < timeout:
        results = worker_pool.process_futures()
        if results:
            return results
        time.sleep(interval)
    raise TimeoutError(f"Timed out after {timeout}s waiting for worker pool results.")

# --- Tests for the Worker Thread Pool and Futures Postprocessing ---

class TestJobManagerWorkerThreadPool:
    def test_worker_thread_lifecycle(self, worker_pool):
        """Test that the worker thread pool starts and shuts down as expected."""
       
        # Before shutdown, the executor should be active
        assert not worker_pool._executor._shutdown
        worker_pool.shutdown()
        assert worker_pool._executor._shutdown


    def test_submit_and_process_successful_task(
        self, worker_pool, valid_task, successful_backend_mock, requests_mock
    ):
        """Test successful submission and processing of a task."""
        # Setup successful backend responses for the valid task.
        successful_backend_mock(valid_task.root_url, valid_task.job_id)
        worker_pool.submit_task(valid_task)
        
        # Wait for the task to complete
        results = wait_for_results(worker_pool)

        # Unpack and assert
        for result in results:
            # Check that we updated the DB to "queued"
            assert result.db_update == {"status": "queued"}

            # Check that the stats_update reflects one "job start"
            assert result.stats_update == {"job start": 1}


    def test_network_failure_in_task(self, worker_pool, valid_task, requests_mock):
        """Test that a task encountering a network failure returns a failed result."""
        # Simulate a connection error
        requests_mock.get(valid_task.root_url,
                          exc=requests.exceptions.ConnectionError("Backend down"))
        worker_pool.submit_task(valid_task)
       
        results = wait_for_results(worker_pool)

        for result in results:
            # On failure we set status to "start_failed"
            assert result.db_update == {"status": "start_failed"}

            # And we increment the "start_job error" counter
            assert result.stats_update == {"start_job error": 1}


    def test_mixed_success_and_failure_tasks(
        self, worker_pool, requests_mock, successful_backend_mock
    ):
        """Test processing multiple tasks with mixed outcomes."""
        # Success case
        task_success = _JobStartTask(
            root_url="https://foo.test",
            bearer_token="token",
            job_id="job-ok"
        )
        successful_backend_mock(task_success.root_url, task_success.job_id)

        # Failure case
        task_fail = _JobStartTask(
            root_url="https://bar.test",
            bearer_token="token",
            job_id="job-fail"
        )
        requests_mock.get(task_fail.root_url,
                          exc=requests.exceptions.ConnectionError("Network error"))

        worker_pool.submit_task(task_success)
        worker_pool.submit_task(task_fail)

        results = wait_for_results(worker_pool)

        # Verify each outcome by job_id
        for result in results:
            if result.job_id == "job-ok":
                assert result.db_update == {"status": "queued"}
                assert result.stats_update == {"job start": 1}
            elif result.job_id == "job-fail":
                assert result.db_update == {"status": "start_failed"}
                assert result.stats_update == {"start_job error": 1}
            else:
                pytest.skip(f"Unexpected task {result.job_id}")

    def test_worker_pool_bookkeeping(self, worker_pool, valid_task, successful_backend_mock, requests_mock):
        """Ensure that processed futures are removed from the pool's internal tracking."""
        successful_backend_mock(valid_task.root_url, valid_task.job_id)
        worker_pool.submit_task(valid_task)
        results = wait_for_results(worker_pool)

        # Assuming your refactoring clears out futures after processing,
        # the internal list (or maps) should be empty.
        assert len(worker_pool._future_task_pairs) == 0
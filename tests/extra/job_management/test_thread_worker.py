import logging
import time
import pytest
import pandas as pd


from openeo.extra.job_management._thread_worker import (
    _JobManagerWorkerThreadPool,
    _JobStartTask,
    _TaskResult,
    _postprocess_futures
)
# --- Fixtures and Helpers ---

@pytest.fixture
def worker_pool():
    """Fixture for creating and cleaning up a worker thread pool."""
    pool = _JobManagerWorkerThreadPool()
    yield pool
    pool.shutdown()

@pytest.fixture
def successful_backend_mock(requests_mock):
    """
    Returns a helper to set up a successful backend.
    Mocks a version check, job start, and job status check.
    """
    def _setup(root_url: str, job_id: str, status: str = "queued"):
        # Backend version check
        requests_mock.get(root_url, json={"api_version": "1.1.0"})
        # Job start
        requests_mock.post(
            f"{root_url}/jobs/{job_id}/results",
            json={"job_id": job_id, "status": status},
            status_code=202
        )
        # Job status check
        requests_mock.get(
            f"{root_url}/jobs/{job_id}",
            json={"job_id": job_id, "status": status}
        )
    return _setup

@pytest.fixture
def valid_task():
    """Fixture to create a valid _JobStartTask instance."""
    return _JobStartTask(
        root_url="https://foo.test",
        bearer_token="test-token",
        job_id="test-job-123"
    )


@pytest.fixture
def sample_dataframe():
    """Creates a pandas DataFrame for job tracking."""
    df = pd.DataFrame([
        {"id": "job-123", "status": "queued_for_start"},
        {"id": "job-456", "status": "queued_for_start"},
        {"id": "job-789", "status": "other"}
    ])
    return df

@pytest.fixture
def initial_stats():
    """Returns a dictionary with initial stats counters."""
    return {"job start": 0, "job start failed": 0}

# --- Test Classes ---

class TestJobManagerWorkerThreadPool:
    def test_worker_thread_lifecycle(self, worker_pool, caplog):
        """Ensure that the thread pool starts and shuts down properly."""
        with caplog.at_level(logging.INFO):
            assert not worker_pool._executor._shutdown
            worker_pool.shutdown()
            assert worker_pool._executor._shutdown
        assert "Shutting down worker thread pool" in caplog.text


    def test_submit_and_process_task(self, worker_pool, valid_task, successful_backend_mock, requests_mock):
        """Test successful task submission and processing in the thread pool."""
        successful_backend_mock(valid_task.root_url, valid_task.job_id)
        worker_pool.submit_task(valid_task)
        
        results = []
        while not results:
            results = worker_pool.process_futures()
            time.sleep(0.1)
        
        # Unpack result
        task, result = results[0]
        
        assert task is valid_task
        assert result.success is True
        assert result.value == "queued"  # Ensure the correct status is returned

    def test_network_failure(self, worker_pool, valid_task, requests_mock):
        """Test handling of a network failure during task execution."""
        requests_mock.get(valid_task.root_url, exc=ConnectionError("Backend down"))
        worker_pool.submit_task(valid_task)
        time.sleep(0.1)
        results = worker_pool.process_futures()

        task, result = results[0]
        assert task is valid_task
        assert result.success is False
        assert "Backend down" in result.value


    def test_mixed_success_and_failure(self, worker_pool, requests_mock, successful_backend_mock):
        """
        Test processing of multiple tasks where one succeeds and another fails.
        """
        # Task that should succeed
        task1 = _JobStartTask(
            root_url="https://foo.test",
            bearer_token="token",
            job_id="job-ok"
        )
        successful_backend_mock(task1.root_url, task1.job_id)

        # Task that should fail
        task2 = _JobStartTask(
            root_url="https://bar.test",
            bearer_token="token",
            job_id="job-fail"
        )
        requests_mock.get(task2.root_url, exc=ConnectionError("Network error"))

        worker_pool.submit_task(task1)
        worker_pool.submit_task(task2)
        time.sleep(0.2)
        results = []
        while len(results) < 2:
            results.extend(worker_pool.process_futures())
            time.sleep(0.1)
        # Verify each task's outcome
        for task, result in results:
            if task.job_id == "job-ok":
                assert result.success is True
            elif task.job_id == "job-fail":
                assert result.success is False

    def test_submit_and_process_multiple_tasks(self, worker_pool, requests_mock, successful_backend_mock):
        """Test submission and processing of multiple tasks concurrently."""
        num_tasks = 5
        tasks = []
        for i in range(num_tasks):
            task = _JobStartTask(
                root_url="https://foo.test",
                bearer_token="token",
                job_id=f"task_{i}"
            )
            tasks.append(task)
            successful_backend_mock(task.root_url, task.job_id)
            worker_pool.submit_task(task)
        results = []
        while len(results) < num_tasks:
            results.extend(worker_pool.process_futures())
            time.sleep(0.1)
        assert len(results) == num_tasks
        for task, result in results:
            assert result.success is True

class TestJobStartTask:
    """Tests for the _JobStartTask class."""

    def test_valid_parameters(self):
        """Test initialization with valid parameters."""
        task = _JobStartTask(
            root_url="https://foo.test",
            bearer_token="valid-token",
            job_id="job-123"
        )
        assert task.root_url == "https://foo.test"
        assert task.bearer_token == "valid-token"
        assert task.job_id == "job-123"

    @pytest.mark.parametrize("invalid_url", [123, None, ""])
    def test_invalid_root_url(self, invalid_url):
        """Test validation of an invalid root_url parameter."""
        with pytest.raises((TypeError, ValueError)) as exc_info:
            _JobStartTask(
                root_url=invalid_url,
                bearer_token="token",
                job_id="job-123"
            )
        assert "root_url must be a non-empty string" in str(exc_info.value)

    @pytest.mark.parametrize("invalid_job_id", [123, None, ""])
    def test_invalid_job_id(self, invalid_job_id):
        """Test validation of an invalid job_id parameter."""
        with pytest.raises((TypeError, ValueError)) as exc_info:
            _JobStartTask(
                root_url="https://foo.test",
                bearer_token="token",
                job_id=invalid_job_id
            )
        assert "job_id must be a non-empty string" in str(exc_info.value)

    def test_execute_success(self, requests_mock, successful_backend_mock):
        """Test successful execution of a job start task."""
        backend_url = "https://foo.test"
        job_id = "job-123"
        successful_backend_mock(backend_url, job_id)
        task = _JobStartTask(
            root_url=backend_url,
            bearer_token="valid-token",
            job_id=job_id
        )
        result = task.execute()

        # Updated assertions based on new return structure
        assert result.success is True
        assert result.value == "queued"

    def test_execute_failure(self, requests_mock):
        """Test execution failure due to network error."""
        backend_url = "https://bar.test"
        job_id = "job-456"
        requests_mock.get(backend_url, exc=ConnectionError("Network failure"))
        task = _JobStartTask(
            root_url=backend_url,
            bearer_token="valid-token",
            job_id=job_id
        )
        result = task.execute()

        # Updated failure assertions
        assert result.success is False
        assert "Network failure" in result.value  # Error message should be stored in `result.value`

    def test_execute_authentication_failure(self, requests_mock):
        """Test execution failure due to authentication issues."""
        backend_url = "https://test.openeo.org"
        job_id = "job-789"
        requests_mock.get(backend_url, json={"api_version": "1.1.0"})
        
        requests_mock.post(
            f"{backend_url}/jobs/{job_id}/results",
            json={"message": "Invalid credentials"},
            status_code=401
        )

        task = _JobStartTask(
            root_url=backend_url,
            bearer_token="invalid-token",
            job_id=job_id
        )
        result = task.execute()

        # Updated authentication failure assertions
        assert result.success is False
        assert "Invalid credentials" in result.value

    
class TestPostProcess:
    """Tests for the _JobStartTask class.""" 

    def test_postprocess_job_start_success(self, sample_dataframe, initial_stats):
        """Ensure _JobStartTask success updates the DataFrame and stats."""
        task = _JobStartTask(root_url="https://foo.test", bearer_token="token", job_id="job-123")
        # Simulate a successful result
        result = _TaskResult(success=True, value="queued")
        
        _postprocess_futures([(task, result)], sample_dataframe, initial_stats)
        
        # The row for job-123 should have its status updated to "queued"
        updated_status = sample_dataframe.loc[sample_dataframe["id"] == "job-123", "status"].iloc[0]
        assert updated_status == "queued"
        # Stats should record one successful job start
        assert initial_stats["job start"] == 1

    def test_postprocess_job_start_failure(self, sample_dataframe, initial_stats):
        """Ensure _JobStartTask failure updates the DataFrame and stats."""
        task = _JobStartTask(root_url="https://foo.test", bearer_token="token", job_id="job-456")
        # Simulate a failing result
        result = _TaskResult(success=False, value="Network error")
        
        _postprocess_futures([(task, result)], sample_dataframe, initial_stats)
        
        # The row for job-456 should have its status updated to "start_failed"
        updated_status = sample_dataframe.loc[sample_dataframe["id"] == "job-456", "status"].iloc[0]
        assert updated_status == "start_failed"
        # Stats should record one failed job start
        assert initial_stats["job start failed"] == 1

    def test_postprocess_no_matching_row(self, sample_dataframe, initial_stats):
        """
        If no row with status 'queued_for_start' exists for the job,
        then _postprocess_futures should not update any status.
        """
        # Create a task for which the DataFrame row has a different status
        task = _JobStartTask(root_url="https://foo.test", bearer_token="token", job_id="job-789")
        result = _TaskResult(success=True, value="queued")
        
        # Before processing, verify job-789 status is "other"
        orig_status = sample_dataframe.loc[sample_dataframe["id"] == "job-789", "status"].iloc[0]
        assert orig_status == "other"
        
        _postprocess_futures([(task, result)], sample_dataframe, initial_stats)
        
        # The status should remain unchanged because it wasn’t "queued_for_start"
        updated_status = sample_dataframe.loc[sample_dataframe["id"] == "job-789", "status"].iloc[0]
        assert updated_status == "other"
        # Stats should  be updated since postprocessing since we did pass a succes.
        assert initial_stats["job start"] == 1


    def test_worker_pool_bookkeeping(self, worker_pool, valid_task, successful_backend_mock, requests_mock):
        """
        Ensure that after processing, the future is removed from the pool's internal maps.
        """
        successful_backend_mock(valid_task.root_url, valid_task.job_id)
        worker_pool.submit_task(valid_task)
        
        # Wait until at least one future is processed.
        results = []
        while not results:
            results = worker_pool.process_futures()
        
        # The internal future list and task map should be empty now.
        assert len(worker_pool._futures) == 0
        assert len(worker_pool._task_map) == 0
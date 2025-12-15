import logging
import threading
import time
from dataclasses import dataclass
from typing import Iterator
from pathlib import Path
import tempfile
from requests_mock import Mocker

import pytest

from openeo.extra.job_management._thread_worker import (
    Task,
    _TaskThreadPool,
    _JobManagerWorkerThreadPool,
    _JobStartTask,
    _TaskResult,
    _JobDownloadTask
)
from openeo.rest._testing import DummyBackend


@pytest.fixture
def dummy_backend(requests_mock) -> DummyBackend:
    dummy = DummyBackend.at_url("https://foo.test", requests_mock=requests_mock)
    dummy.setup_simple_job_status_flow(queued=2, running=5)
    return dummy


class TestTaskResult:
    def test_default(self):
        result = _TaskResult(job_id="j-123", df_idx=0)
        assert result.job_id == "j-123"
        assert result.df_idx == 0
        assert result.db_update == {}
        assert result.stats_update == {}


class TestJobStartTask:
    def test_start_success(self, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        job = dummy_backend.connection.create_job(process_graph={})

        task = _JobStartTask(
            job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token="h4ll0"
        )
        result = task.execute()

        assert result == _TaskResult(
            job_id="job-000",
            df_idx=0,
            db_update={"status": "queued"},
            stats_update={"job start": 1},
        )
        assert job.status() == "queued"
        assert caplog.messages == []

    def test_start_failure(self, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        job = dummy_backend.connection.create_job(process_graph={})
        dummy_backend.setup_job_start_failure()

        task = _JobStartTask(
            job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token="h4ll0"
        )
        result = task.execute()

        assert result == _TaskResult(
            job_id="job-000",
            df_idx=0,
            db_update={"status": "start_failed"},
            stats_update={"start_job error": 1},
        )
        assert job.status() == "error"
        assert caplog.messages == [
            "Failed to start job 'job-000': OpenEoApiError('[500] Internal: No job starting for you, buddy')"
        ]

    @pytest.mark.parametrize("serializer", [repr, str])
    def test_hide_token(self, serializer):
        secret = "Secret!"
        task = _JobStartTask(job_id="job-123", df_idx=0, root_url="https://example.com", bearer_token=secret)
        serialized = serializer(task)
        assert "job-123" in serialized
        assert secret not in serialized

class TestJobDownloadTask:
    
    # Use a temporary directory for safe file handling
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    def test_job_download_success(self, requests_mock: Mocker, temp_dir: Path):
        """
        Test a successful job download and verify file content and stats update.
        """
        job_id = "job-007"
        df_idx = 42
        
        # We set up a dummy backend to simulate the job results and assert the expected calls are triggered
        backend = DummyBackend.at_url("https://openeo.dummy.test/", requests_mock=requests_mock)
        backend.next_result = b"The downloaded file content."
        backend.batch_jobs[job_id] = {"job_id": job_id, "pg": {}, "status": "created"}
        
        backend._set_job_status(job_id=job_id, status="finished")
        backend.batch_jobs[job_id]["status"] = "finished"  

        download_dir = temp_dir / job_id / "results"
        download_dir.mkdir(parents=True)
        
        # Create the task instance
        task = _JobDownloadTask(
            root_url="https://openeo.dummy.test/",
            bearer_token="dummy-token-7",
            job_id=job_id,
            df_idx=df_idx,
            download_dir=download_dir,
        )

        # Execute the task
        result = task.execute()

        # Verify TaskResult structure
        assert isinstance(result, _TaskResult)
        assert result.job_id == job_id
        assert result.df_idx == df_idx
        
        # Verify stats update for the MultiBackendJobManager
        assert result.stats_update == {"job download": 1}
        
        # Verify download content (crucial part of the unit test)
        downloaded_file = download_dir / "result.data"
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == b"The downloaded file content."

        
    def test_job_download_failure(self, requests_mock: Mocker, temp_dir: Path):
        """
        Test a failed download (e.g., bad connection) and verify error reporting.
        """
        job_id = "job-008"
        df_idx = 55
                
        # Set up dummy backend to simulate failure during results listing
        backend = DummyBackend.at_url("https://openeo.dummy.test/", requests_mock=requests_mock)

        #simulate and error when downloading the results
        requests_mock.get(
            f"https://openeo.dummy.test/jobs/{job_id}/results",
            status_code=500,
            json={"code": "InternalError", "message": "Failed to list results"})

        backend.batch_jobs[job_id] = {"job_id": job_id, "pg": {}, "status": "created"}
        backend._set_job_status(job_id=job_id, status="finished")
        backend.batch_jobs[job_id]["finished"] = "error"
        
        download_dir = temp_dir / job_id / "results"
        download_dir.mkdir(parents=True)

        # Create the task instance
        task = _JobDownloadTask(
            root_url="https://openeo.dummy.test/",
            bearer_token="dummy-token-8",
            job_id=job_id,
            df_idx=df_idx,
            download_dir=download_dir,
        )

        # Execute the task
        result = task.execute()

        # Verify TaskResult structure
        assert isinstance(result, _TaskResult)
        assert result.job_id == job_id
        assert result.df_idx == df_idx
        
        # Verify stats update for the MultiBackendJobManager
        assert result.stats_update == {"job download error": 1}
        
        # Verify no file was created (or only empty/failed files)
        assert not any(p.is_file() for p in download_dir.glob("*"))

class NopTask(Task):
    """Do Nothing"""

    def execute(self) -> _TaskResult:
        return _TaskResult(job_id=self.job_id, df_idx=self.df_idx)


class DummyTask(Task):
    def execute(self) -> _TaskResult:
        if self.job_id == "j-666":
            raise ValueError("Oh no!")
        return _TaskResult(
            job_id=self.job_id,
            df_idx=self.df_idx,
            db_update={"status": "dummified"},
            stats_update={"dummy": 1},
        )


@dataclass(frozen=True)
class BlockingTask(Task):
    """Another dummy task that blocks until an event is set, and optionally fails."""

    event: threading.Event
    timeout: int = 5
    success: bool = True

    def execute(self) -> _TaskResult:
        released = self.event.wait(timeout=self.timeout)
        if not released:
            raise TimeoutError("Waiting for event timed out")
        if not self.success:
            raise ValueError("Oh no!")
        return _TaskResult(job_id=self.job_id, df_idx=self.df_idx, db_update={"status": "all fine"})


class TestTaskThreadPool:
    @pytest.fixture
    def worker_pool(self) -> Iterator[_TaskThreadPool]:
        """Fixture for creating and cleaning up a worker thread pool."""
        pool = _TaskThreadPool()
        yield pool
        pool.shutdown()

    def test_no_tasks(self, worker_pool):
        results, remaining = worker_pool.process_futures(timeout=10)
        assert results == []
        assert remaining == 0

    def test_submit_and_process(self, worker_pool):
        worker_pool.submit_task(DummyTask(job_id="j-123", df_idx=0))
        results, remaining = worker_pool.process_futures(timeout=10)
        assert results == [
            _TaskResult(job_id="j-123", df_idx=0, db_update={"status": "dummified"}, stats_update={"dummy": 1}),
        ]
        assert remaining == 0

    def test_submit_and_process_zero_timeout(self, worker_pool):
        worker_pool.submit_task(DummyTask(job_id="j-123", df_idx=0))
        # Trigger context switch
        time.sleep(0.1)
        results, remaining = worker_pool.process_futures(timeout=0)
        assert results == [
            _TaskResult(job_id="j-123", df_idx=0, db_update={"status": "dummified"}, stats_update={"dummy": 1}),
        ]
        assert remaining == 0

    def test_submit_and_process_with_error(self, worker_pool):
        worker_pool.submit_task(DummyTask(job_id="j-666", df_idx=0))
        results, remaining = worker_pool.process_futures(timeout=10)
        assert results == [
            _TaskResult(
                job_id="j-666",
                df_idx=0,
                db_update={"status": "threaded task failed"},
                stats_update={"threaded task failed": 1},
            ),
        ]
        assert remaining == 0

    def test_submit_and_process_iterative(self, worker_pool):
        worker_pool.submit_task(NopTask(job_id="j-1", df_idx=1))
        results, remaining = worker_pool.process_futures(timeout=1)
        assert results == [_TaskResult(job_id="j-1", df_idx=1)]
        assert remaining == 0

        # Add some more
        worker_pool.submit_task(NopTask(job_id="j-22", df_idx=22))
        worker_pool.submit_task(NopTask(job_id="j-222", df_idx=222))
        results, remaining = worker_pool.process_futures(timeout=1)
        assert results == [_TaskResult(job_id="j-22", df_idx=22), _TaskResult(job_id="j-222", df_idx=222)]
        assert remaining == 0

    def test_submit_multiple_simple(self, worker_pool):
        # A bunch of dummy tasks
        for j in range(5):
            worker_pool.submit_task(NopTask(job_id=f"j-{j}", df_idx=j))

        # Process all of them (non-zero timeout, which should be plenty of time for all of them to finish)
        results, remaining = worker_pool.process_futures(timeout=1)
        expected = [_TaskResult(job_id=f"j-{j}", df_idx=j) for j in range(5)]
        assert sorted(results, key=lambda r: r.job_id) == expected

    def test_submit_multiple_blocking_and_failing(self, worker_pool):
        # Setup bunch of blocking tasks, some failing
        events = []
        n = 5
        for j in range(n):
            event = threading.Event()
            events.append(event)
            worker_pool.submit_task(
                BlockingTask(
                    job_id=f"j-{j}",
                    df_idx=j,
                    event=event,
                    success=j != 3,
                )
            )

        # Initial state: nothing happened yet
        results, remaining = worker_pool.process_futures(timeout=0)
        assert (results, remaining) == ([], n)

        # No changes even after timeout
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert (results, remaining) == ([], n)

        # Set one event and wait for corresponding result
        events[0].set()
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [
            _TaskResult(job_id="j-0", df_idx=0, db_update={"status": "all fine"}),
        ]
        assert remaining == n - 1

        # Release all but one event
        for j in range(n - 1):
            events[j].set()
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [
            _TaskResult(job_id="j-1", df_idx=1, db_update={"status": "all fine"}),
            _TaskResult(job_id="j-2", df_idx=2, db_update={"status": "all fine"}),
            _TaskResult(
                job_id="j-3",
                df_idx=3,
                db_update={"status": "threaded task failed"},
                stats_update={"threaded task failed": 1},
            ),
        ]
        assert remaining == 1

        # Release all events
        for j in range(n):
            events[j].set()
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [
            _TaskResult(job_id="j-4", df_idx=4, db_update={"status": "all fine"}),
        ]
        assert remaining == 0

    def test_shutdown(self, worker_pool):
        # Before shutdown
        worker_pool.submit_task(NopTask(job_id="j-123", df_idx=0))
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert (results, remaining) == ([_TaskResult(job_id="j-123", df_idx=0)], 0)

        worker_pool.shutdown()

        # After shutdown, no new tasks should be accepted
        with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
            worker_pool.submit_task(NopTask(job_id="j-456", df_idx=1))

    def test_job_start_task(self, worker_pool, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        job = dummy_backend.connection.create_job(process_graph={})
        task = _JobStartTask(job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token=None)
        worker_pool.submit_task(task)

        results, remaining = worker_pool.process_futures(timeout=1)
        assert results == [
            _TaskResult(
                job_id="job-000",
                df_idx=0,
                db_update={"status": "queued"},
                stats_update={"job start": 1},
            )
        ]
        assert remaining == 0
        assert caplog.messages == []

    def test_job_start_task_failure(self, worker_pool, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        dummy_backend.setup_job_start_failure()

        job = dummy_backend.connection.create_job(process_graph={})
        task = _JobStartTask(job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token=None)
        worker_pool.submit_task(task)

        results, remaining = worker_pool.process_futures(timeout=1)
        assert results == [
            _TaskResult(
                job_id="job-000", df_idx=0, db_update={"status": "start_failed"}, stats_update={"start_job error": 1}
            )
        ]
        assert remaining == 0
        assert caplog.messages == [
            "Failed to start job 'job-000': OpenEoApiError('[500] Internal: No job starting for you, buddy')"
        ]


import pytest
import time
import threading
import logging
from typing import Iterator

_log = logging.getLogger(__name__)


class TestJobManagerWorkerThreadPool:
    @pytest.fixture
    def thread_pool(self) -> Iterator[_JobManagerWorkerThreadPool]:
        """Fixture for creating and cleaning up a thread pool manager."""
        pool = _JobManagerWorkerThreadPool()
        yield pool
        pool.shutdown()

    @pytest.fixture
    def configured_pool(self) -> Iterator[_JobManagerWorkerThreadPool]:
        """Fixture with pre-configured pools."""
        pool = _JobManagerWorkerThreadPool(
            pool_configs={
                "NopTask": 2,
                "DummyTask": 3,
                "BlockingTask": 1,
            }
        )
        yield pool
        pool.shutdown()

    def test_init_empty_config(self):
        """Test initialization with empty config."""
        pool = _JobManagerWorkerThreadPool()
        assert pool._pools == {}
        assert pool._pool_configs == {}
        pool.shutdown()

    def test_init_with_config(self):
        """Test initialization with pool configurations."""
        pool = _JobManagerWorkerThreadPool({
            "NopTask": 2,
            "DummyTask": 3,
        })
        # Pools should NOT be created until first use
        assert pool._pools == {}
        assert pool._pool_configs == {
            "NopTask": 2,
            "DummyTask": 3,
        }
        pool.shutdown()

    def test_submit_task_creates_pool(self, thread_pool):
        """Test that submitting a task creates a pool dynamically."""
        task = NopTask(job_id="j-1", df_idx=1)
        
        # No pools initially
        assert thread_pool.list_pools() == []
        
        # Submit task - should create pool
        thread_pool.submit_task(task)
        
        # Pool should be created with default workers (1)
        assert thread_pool.list_pools() == ["NopTask"]
        assert "NopTask" in thread_pool._pools
        
        # Process to complete the task
        results, remaining = thread_pool.process_all_updates(timeout=0.1)
        assert len(results) == 1
        assert results[0].job_id == "j-1"
        assert remaining == {"NopTask": 0}

    def test_submit_task_uses_config(self, configured_pool):
        """Test that pool creation uses configuration."""
        task = NopTask(job_id="j-1", df_idx=1)
        
        # Submit task - should create pool with configured workers
        configured_pool.submit_task(task)
        
        assert "NopTask" in configured_pool._pools
        # Can't directly check max_workers, but pool should exist
        assert "NopTask" in configured_pool.list_pools()

    def test_submit_multiple_task_types(self, thread_pool):
        """Test submitting different task types to different pools."""
        # Submit different task types
        task1 = NopTask(job_id="j-1", df_idx=1)
        task2 = DummyTask(job_id="j-2", df_idx=2)
        task3 = NopTask(job_id="j-3", df_idx=3)
        
        thread_pool.submit_task(task1)  # Goes to "NopTask" pool
        thread_pool.submit_task(task2)  # Goes to "DummyTask" pool  
        thread_pool.submit_task(task3)  # Goes to "NopTask" pool (existing)
        
        # Should have 2 pools
        pools = sorted(thread_pool.list_pools())
        assert pools == ["DummyTask", "NopTask"]
        
        # Check pending tasks
        assert thread_pool.num_pending_tasks() == 3
        assert thread_pool.num_pending_tasks("NopTask") == 2
        assert thread_pool.num_pending_tasks("DummyTask") == 1
        assert thread_pool.num_pending_tasks("NonExistent") == 0

    def test_process_all_updates_empty(self, thread_pool):
        """Test processing updates with no pools."""
        results, remaining = thread_pool.process_all_updates(timeout=0)
        assert results == []
        assert remaining == {}

    def test_process_all_updates_multiple_pools(self, thread_pool):
        """Test processing updates across multiple pools."""
        # Submit tasks to different pools
        thread_pool.submit_task(NopTask(job_id="j-1", df_idx=1))  # NopTask pool
        thread_pool.submit_task(NopTask(job_id="j-2", df_idx=2))  # NopTask pool
        thread_pool.submit_task(DummyTask(job_id="j-3", df_idx=3))  # DummyTask pool
        
        # Process updates
        results, remaining = thread_pool.process_all_updates(timeout=0.1)
        
        # Should get 3 results
        assert len(results) == 3
        # Check results by pool
        nop_results = [r for r in results if r.job_id in ["j-1", "j-2"]]
        dummy_results = [r for r in results if r.job_id == "j-3"]
        assert len(nop_results) == 2
        assert len(dummy_results) == 1
        
        # All tasks should be completed
        assert remaining == {"NopTask": 0, "DummyTask": 0}

    def test_process_all_updates_partial_completion(self):
        """Test processing when some tasks are still running."""
        # Use a pool with blocking tasks
        pool = _JobManagerWorkerThreadPool()
        
        # Create a blocking task
        event = threading.Event()
        blocking_task = BlockingTask(job_id="j-block", df_idx=0, event=event, success=True)
        
        # Create a quick task
        quick_task = NopTask(job_id="j-quick", df_idx=1)
        
        pool.submit_task(blocking_task)  # BlockingTask pool
        pool.submit_task(quick_task)     # NopTask pool
        
        # Process with timeout=0 - only quick task should complete
        results, remaining = pool.process_all_updates(timeout=0)
        
        # Only quick task completed
        assert len(results) == 1
        assert results[0].job_id == "j-quick"
        
        # Blocking task still pending
        assert remaining == {"BlockingTask": 1, "NopTask": 0}
        assert pool.num_pending_tasks() == 1
        assert pool.num_pending_tasks("BlockingTask") == 1
        
        # Release blocking task and process again
        event.set()
        results2, remaining2 = pool.process_all_updates(timeout=0.1)
        
        assert len(results2) == 1
        assert results2[0].job_id == "j-block"
        assert remaining2 == {"BlockingTask": 0, "NopTask": 0}
        
        pool.shutdown()

    def test_num_pending_tasks(self, thread_pool):
        """Test counting pending tasks."""
        # Initially empty
        assert thread_pool.num_pending_tasks() == 0
        assert thread_pool.num_pending_tasks("NopTask") == 0
        
        # Add some tasks
        thread_pool.submit_task(NopTask(job_id="j-1", df_idx=1))
        thread_pool.submit_task(NopTask(job_id="j-2", df_idx=2))
        thread_pool.submit_task(DummyTask(job_id="j-3", df_idx=3))
        
        # Check totals
        assert thread_pool.num_pending_tasks() == 3
        assert thread_pool.num_pending_tasks("NopTask") == 2
        assert thread_pool.num_pending_tasks("DummyTask") == 1
        assert thread_pool.num_pending_tasks("NonExistentPool") == 0
        
        # Process all
        thread_pool.process_all_updates(timeout=0.1)
        
        # Should be empty
        assert thread_pool.num_pending_tasks() == 0
        assert thread_pool.num_pending_tasks("NopTask") == 0

    def test_shutdown_specific_pool(self):
        """Test shutting down a specific pool."""
        # Create fresh pool for destructive test
        pool = _JobManagerWorkerThreadPool()
        
        # Create two pools
        pool.submit_task(NopTask(job_id="j-1", df_idx=1))  # NopTask pool
        pool.submit_task(DummyTask(job_id="j-2", df_idx=2))  # DummyTask pool
        
        assert sorted(pool.list_pools()) == ["DummyTask", "NopTask"]
        
        # Shutdown NopTask pool only
        pool.shutdown("NopTask")
        
        # Only DummyTask pool should remain
        assert pool.list_pools() == ["DummyTask"]
        
        # Can't submit to shutdown pool
        # Actually, it will create a new pool since we deleted it
        pool.submit_task(NopTask(job_id="j-3", df_idx=3))  # Creates new NopTask pool
        assert sorted(pool.list_pools()) == ["DummyTask", "NopTask"]
        
        pool.shutdown()

    def test_shutdown_all(self):
        """Test shutting down all pools."""
        # Create fresh pool for destructive test
        pool = _JobManagerWorkerThreadPool()
        
        # Create multiple pools
        pool.submit_task(NopTask(job_id="j-1", df_idx=1))
        pool.submit_task(DummyTask(job_id="j-2", df_idx=2))
        
        assert len(pool.list_pools()) == 2
        
        # Shutdown all
        pool.shutdown()
        
        # All pools should be gone
        assert pool.list_pools() == []
        
        # Can't submit any more tasks after shutdown
        # Actually, shutdown() doesn't prevent creating new pools
        # So we can test that shutdown clears existing pools
        assert len(pool._pools) == 0

    def test_custom_get_pool_name(self):
        """Test custom task class to verify pool name selection."""
        
        @dataclass(frozen=True)
        class CustomTask(Task):
            # Fields are inherited from Task: job_id, df_idx
            
            def execute(self) -> _TaskResult:
                return _TaskResult(job_id=self.job_id, df_idx=self.df_idx)
        
        pool = _JobManagerWorkerThreadPool()
        
        # Submit custom task - must provide all required fields
        task = CustomTask(job_id="j-1", df_idx=1)
        pool.submit_task(task)
        
        # Pool should be named after class
        assert pool.list_pools() == ["CustomTask"]
        assert pool.num_pending_tasks() == 1
        
        # Process it
        results, remaining = pool.process_all_updates(timeout=0.1)
        assert len(results) == 1
        assert results[0].job_id == "j-1"
        
        pool.shutdown()

    def test_concurrent_submissions(self, thread_pool):
        """Test concurrent task submissions to same pool."""
        import concurrent.futures
        
        def submit_tasks(start_idx: int):
            for i in range(5):
                thread_pool.submit_task(NopTask(job_id=f"j-{start_idx + i}", df_idx=start_idx + i))
        
        # Submit tasks from multiple threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(submit_tasks, i * 10) for i in range(3)]
            concurrent.futures.wait(futures)
        
        # Should have all tasks in one pool
        assert thread_pool.list_pools() == ["NopTask"]
        assert thread_pool.num_pending_tasks() == 15
        
        # Process them all
        results, remaining = thread_pool.process_all_updates(timeout=0.5)
        
        assert len(results) == 15
        assert remaining == {"NopTask": 0}

    def test_pool_parallelism_with_blocking_tasks(self):
        """Test that multiple workers allow parallel execution."""
        pool = _JobManagerWorkerThreadPool({
            "BlockingTask": 3,  # 3 workers for blocking tasks
        })
        
        # Create multiple blocking tasks
        events = [threading.Event() for _ in range(5)]
        start_time = time.time()
        
        for i, event in enumerate(events):
            pool.submit_task(BlockingTask(
                job_id=f"j-block-{i}", 
                df_idx=i, 
                event=event,
                success=True
            ))
        
        # Initially all pending
        assert pool.num_pending_tasks() == 5
        
        # Release all events at once
        for event in events:
            event.set()
        
        # Process with timeout - all should complete
        results, remaining = pool.process_all_updates(timeout=0.5)
        
        # All should complete (if pool had enough workers)
        assert len(results) == 5
        assert remaining == {"BlockingTask": 0}
        
        # Check they all completed
        for result in results:
            assert result.job_id.startswith("j-block-")
        
        pool.shutdown()

    def test_task_with_error_handling(self, thread_pool):
        """Test that task errors are properly handled in the pool."""
        # Submit a failing DummyTask (j-666 fails)
        thread_pool.submit_task(DummyTask(job_id="j-666", df_idx=0))
        
        # Process it
        results, remaining = thread_pool.process_all_updates(timeout=0.1)
        
        # Should get error result
        assert len(results) == 1
        result = results[0]
        assert result.job_id == "j-666"
        assert result.db_update == {"status": "threaded task failed"}
        assert result.stats_update == {"threaded task failed": 1}
        assert remaining == {"DummyTask": 0}

    def test_mixed_success_and_error_tasks(self, thread_pool):
        """Test mix of successful and failing tasks."""
        # Submit mix of tasks
        thread_pool.submit_task(DummyTask(job_id="j-1", df_idx=1))   # Success
        thread_pool.submit_task(DummyTask(job_id="j-666", df_idx=2)) # Failure  
        thread_pool.submit_task(DummyTask(job_id="j-3", df_idx=3))   # Success
        
        # Process all
        results, remaining = thread_pool.process_all_updates(timeout=0.1)
        
        # Should get 3 results
        assert len(results) == 3
        assert remaining == {"DummyTask": 0}
        
        # Check results
        success_results = [r for r in results if r.job_id != "j-666"]
        error_results = [r for r in results if r.job_id == "j-666"]
        
        assert len(success_results) == 2
        assert len(error_results) == 1
        
        # Verify success results
        for result in success_results:
            assert result.db_update == {"status": "dummified"}
            assert result.stats_update == {"dummy": 1}
        
        # Verify error result
        error_result = error_results[0]
        assert error_result.db_update == {"status": "threaded task failed"}
        assert error_result.stats_update == {"threaded task failed": 1}
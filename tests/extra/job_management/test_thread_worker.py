import logging
import threading
import time
from dataclasses import dataclass
from typing import Iterator
from pathlib import Path
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
    

    def test_job_download_success(self, requests_mock: Mocker, tmp_path: Path):
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

        download_dir = tmp_path / job_id / "results"
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
        assert result.stats_update == {'files downloaded': 1, "job download": 1}
        
        # Verify download content (crucial part of the unit test)
        downloaded_file = download_dir / "result.data"
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == b"The downloaded file content."

        
    def test_job_download_failure(self, requests_mock: Mocker, tmp_path: Path):
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
        
        download_dir = tmp_path / job_id / "results"
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
        assert result.stats_update == {'files downloaded': 0, "job download error": 1}
        
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
        assert worker_pool.get_unprocessed_count() == 0
        assert worker_pool.has_unprocessed_tasks() == False

    def test_submit_and_process(self, worker_pool):
        worker_pool.submit_task(DummyTask(job_id="j-123", df_idx=0))
        # Check unprocessed before processing
        assert worker_pool.get_unprocessed_count() == 1
        assert worker_pool.has_unprocessed_tasks() == True
        
        results, remaining = worker_pool.process_futures(timeout=10)
        assert results == [
            _TaskResult(job_id="j-123", df_idx=0, db_update={"status": "dummified"}, stats_update={"dummy": 1}),
        ]
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0  # Now processed
        assert worker_pool.has_unprocessed_tasks() == False

    def test_submit_and_process_zero_timeout(self, worker_pool):
        worker_pool.submit_task(DummyTask(job_id="j-123", df_idx=0))
        # Check unprocessed before processing
        assert worker_pool.get_unprocessed_count() == 1
        assert worker_pool.has_unprocessed_tasks() == True
        
        # Trigger context switch
        time.sleep(0.1)
        results, remaining = worker_pool.process_futures(timeout=0)
        assert results == [
            _TaskResult(job_id="j-123", df_idx=0, db_update={"status": "dummified"}, stats_update={"dummy": 1}),
        ]
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0
        assert worker_pool.has_unprocessed_tasks() == False

    def test_submit_and_process_with_error(self, worker_pool):
        worker_pool.submit_task(DummyTask(job_id="j-666", df_idx=0))
        # Check unprocessed before processing
        assert worker_pool.get_unprocessed_count() == 1
        assert worker_pool.has_unprocessed_tasks() == True
        
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
        assert worker_pool.get_unprocessed_count() == 0
        assert worker_pool.has_unprocessed_tasks() == False

    def test_submit_and_process_iterative(self, worker_pool):
        worker_pool.submit_task(NopTask(job_id="j-1", df_idx=1))
        assert worker_pool.get_unprocessed_count() == 1
        
        results, remaining = worker_pool.process_futures(timeout=1)
        assert results == [_TaskResult(job_id="j-1", df_idx=1)]
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0

        # Add some more
        worker_pool.submit_task(NopTask(job_id="j-22", df_idx=22))
        worker_pool.submit_task(NopTask(job_id="j-222", df_idx=222))
        assert worker_pool.get_unprocessed_count() == 2
        
        results, remaining = worker_pool.process_futures(timeout=1)
        assert results == [_TaskResult(job_id="j-22", df_idx=22), _TaskResult(job_id="j-222", df_idx=222)]
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0

    def test_submit_multiple_simple(self, worker_pool):
        # A bunch of dummy tasks
        for j in range(5):
            worker_pool.submit_task(NopTask(job_id=f"j-{j}", df_idx=j))

        # Check unprocessed before processing
        assert worker_pool.get_unprocessed_count() == 5
        assert worker_pool.has_unprocessed_tasks() == True
        
        # Process all of them
        results, remaining = worker_pool.process_futures(timeout=1)
        expected = [_TaskResult(job_id=f"j-{j}", df_idx=j) for j in range(5)]
        assert sorted(results, key=lambda r: r.job_id) == expected
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0
        assert worker_pool.has_unprocessed_tasks() == False

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

        # Initial state: all tasks submitted but not processed
        assert worker_pool.get_unprocessed_count() == n
        assert worker_pool.has_unprocessed_tasks() == True
        
        results, remaining = worker_pool.process_futures(timeout=0)
        assert results == []  # No tasks completed yet
        assert remaining == n  # All still in queue
        assert worker_pool.get_unprocessed_count() == n  # Still unprocessed

        # No changes even after timeout
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == []
        assert remaining == n
        assert worker_pool.get_unprocessed_count() == n

        # Set one event and wait for corresponding result
        events[0].set()
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [
            _TaskResult(job_id="j-0", df_idx=0, db_update={"status": "all fine"}),
        ]
        assert remaining == n - 1
        assert worker_pool.get_unprocessed_count() == n - 1  # One processed

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
        assert remaining == 1  # Only j-4 still in queue
        assert worker_pool.get_unprocessed_count() == 1  # Only j-4 not processed yet

        # Release all events
        for j in range(n):
            events[j].set()
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [
            _TaskResult(job_id="j-4", df_idx=4, db_update={"status": "all fine"}),
        ]
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0  # All processed
        assert worker_pool.has_unprocessed_tasks() == False

    def test_shutdown(self, worker_pool):
        # Before shutdown
        worker_pool.submit_task(NopTask(job_id="j-123", df_idx=0))
        assert worker_pool.get_unprocessed_count() == 1
        
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [_TaskResult(job_id="j-123", df_idx=0)]
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0

        worker_pool.shutdown()

        # After shutdown, no new tasks should be accepted
        with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
            worker_pool.submit_task(NopTask(job_id="j-456", df_idx=1))

    def test_job_start_task(self, worker_pool, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        job = dummy_backend.connection.create_job(process_graph={})
        task = _JobStartTask(job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token=None)
        worker_pool.submit_task(task)
        
        assert worker_pool.get_unprocessed_count() == 1

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
        assert worker_pool.get_unprocessed_count() == 0
        assert caplog.messages == []

    def test_job_start_task_failure(self, worker_pool, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        dummy_backend.setup_job_start_failure()

        job = dummy_backend.connection.create_job(process_graph={})
        task = _JobStartTask(job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token=None)
        worker_pool.submit_task(task)
        
        assert worker_pool.get_unprocessed_count() == 1

        results, remaining = worker_pool.process_futures(timeout=1)
        assert results == [
            _TaskResult(
                job_id="job-000", df_idx=0, db_update={"status": "start_failed"}, stats_update={"start_job error": 1}
            )
        ]
        assert remaining == 0
        assert worker_pool.get_unprocessed_count() == 0
        assert caplog.messages == [
            "Failed to start job 'job-000': OpenEoApiError('[500] Internal: No job starting for you, buddy')"
        ]


    
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
        assert pool.get_unprocessed_counts() == {}
        assert pool.has_unprocessed_tasks() == False
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
        assert pool.get_unprocessed_counts() == {}
        assert pool.has_unprocessed_tasks() == False
        pool.shutdown()

    def test_submit_task_creates_pool(self, thread_pool):
        """Test that submitting a task creates a pool dynamically."""
        task = NopTask(job_id="j-1", df_idx=1)
        
        assert thread_pool.list_pools() == []
        assert thread_pool.has_unprocessed_tasks() == False
        
        # Submit task - should create pool
        thread_pool.submit_task(task)
        
        # Pool should be created
        assert thread_pool.list_pools() == ["default"]
        assert "default" in thread_pool._pools
        assert thread_pool.has_unprocessed_tasks() == True
        assert thread_pool.get_unprocessed_counts()["default"] == 1
        
        # Process to complete the task
        results, remaining = thread_pool.process_futures(timeout=0.1)
        assert len(results) == 1
        assert results[0].job_id == "j-1"
        assert remaining["default"] == 0
        assert thread_pool.has_unprocessed_tasks() == False
        assert thread_pool.get_unprocessed_counts()["default"] == 0

    def test_submit_task_uses_config(self, configured_pool):
        """Test that pool creation uses configuration."""
        task = NopTask(job_id="j-1", df_idx=1)
        
        # Submit task - should create pool with configured workers
        configured_pool.submit_task(task, "NopTask")

        assert "NopTask" in configured_pool._pools
        assert "NopTask" in configured_pool.list_pools()
        assert "DummyTask" not in configured_pool.list_pools()
        assert configured_pool.has_unprocessed_tasks() == True
        assert configured_pool.get_unprocessed_counts()["NopTask"] == 1

    def test_submit_multiple_task_types(self, thread_pool):
        """Test submitting different task types to different pools."""
        # Submit different task types
        task1 = NopTask(job_id="j-1", df_idx=1)
        task2 = DummyTask(job_id="j-2", df_idx=2)
        task3 = DummyTask(job_id="j-3", df_idx=3)
        
        thread_pool.submit_task(task1)  # Goes to default pool
        thread_pool.submit_task(task2)  # Goes to default pool  
        thread_pool.submit_task(task3, "seperate")  # Goes to "seperate" pool

        assert thread_pool.has_unprocessed_tasks() == True
        assert thread_pool.get_unprocessed_counts()["default"] == 2
        assert thread_pool.get_unprocessed_counts()["seperate"] == 1
        
        # Should have 2 pools
        pools = sorted(thread_pool.list_pools())
        assert pools == ["default", "seperate"]

        results, remaining = thread_pool.process_futures(timeout=0)
        
        # Check unprocessed tasks
        assert len(results) == 3  # All tasks completed
        assert sum(remaining.values()) == 0  # No tasks left in queue after processing
        assert thread_pool.has_unprocessed_tasks() == False
        assert thread_pool.get_unprocessed_counts()["default"] == 0
        assert thread_pool.get_unprocessed_counts()["seperate"] == 0

    def test_process_futures_updates_empty(self, thread_pool):
        """Test process futures with no pools."""
        results, remaining = thread_pool.process_futures(timeout=0)
        assert results == []
        assert remaining == {}
        assert thread_pool.has_unprocessed_tasks() == False

    def test_process_futures_updates_multiple_pools(self, thread_pool):
        """Test processing updates across multiple pools."""
        # Submit tasks to different pools
        thread_pool.submit_task(NopTask(job_id="j-1", df_idx=1))  # default pool
        thread_pool.submit_task(NopTask(job_id="j-2", df_idx=2))  # default pool
        thread_pool.submit_task(DummyTask(job_id="j-3", df_idx=3), "dummy")  # dummy pool
        
        # Check before processing
        assert thread_pool.has_unprocessed_tasks() == True
        counts = thread_pool.get_unprocessed_counts()
        assert counts["default"] == 2
        assert counts["dummy"] == 1
        
        results, remaining = thread_pool.process_futures(timeout=0.1)
        
        assert len(results) == 3
        assert remaining["default"] == 0
        assert remaining["dummy"] == 0
        assert thread_pool.has_unprocessed_tasks() == False
        assert thread_pool.get_unprocessed_counts()["default"] == 0
        assert thread_pool.get_unprocessed_counts()["dummy"] == 0

    def test_process_futures_updates_partial_completion(self):
        """Test processing when some tasks are still running."""
        # Use a pool with blocking tasks
        pool = _JobManagerWorkerThreadPool()
        
        # Create a blocking task
        event = threading.Event()
        blocking_task = BlockingTask(job_id="j-block", df_idx=0, event=event, success=True)
        
        # Create a quick task
        quick_task = NopTask(job_id="j-quick", df_idx=1)
        
        pool.submit_task(blocking_task, "blocking")  # blocking pool
        pool.submit_task(quick_task, "quick")        # quick pool
        
        # Check before processing
        assert pool.has_unprocessed_tasks() == True
        assert pool.get_unprocessed_counts()["blocking"] == 1
        assert pool.get_unprocessed_counts()["quick"] == 1
        
        # Process with timeout=0
        results, remaining = pool.process_futures(timeout=0)
        
        # Check what completed
        completed_ids = {r.job_id for r in results}
        
        if "j-quick" in completed_ids:
            # Quick task completed, blocking still in queue
            assert "j-block" not in completed_ids
            assert remaining["blocking"] == 1
            assert remaining["quick"] == 0
            assert pool.has_unprocessed_tasks() == True
            assert pool.get_unprocessed_counts()["blocking"] == 1
            assert pool.get_unprocessed_counts()["quick"] == 0
            
            # Release blocking task and process again
            event.set()
            results2, remaining2 = pool.process_futures(timeout=0.1)
            
            assert len(results2) == 1
            assert results2[0].job_id == "j-block"
            assert remaining2["blocking"] == 0
            assert pool.has_unprocessed_tasks() == False
            assert pool.get_unprocessed_counts()["blocking"] == 0
        else:
            # Both might complete immediately
            assert len(results) == 2
            assert remaining["blocking"] == 0
            assert remaining["quick"] == 0
            assert pool.has_unprocessed_tasks() == False
            assert pool.get_unprocessed_counts()["blocking"] == 0
            assert pool.get_unprocessed_counts()["quick"] == 0
        
        pool.shutdown()

    def test_get_unprocessed_counts(self, thread_pool):
        """Test getting unprocessed counts."""
        # Initially empty
        assert thread_pool.get_unprocessed_counts() == {}
        assert thread_pool.has_unprocessed_tasks() == False
        
        # Add some tasks
        thread_pool.submit_task(NopTask(job_id="j-1", df_idx=1))
        thread_pool.submit_task(NopTask(job_id="j-2", df_idx=2))
        thread_pool.submit_task(DummyTask(job_id="j-3", df_idx=3), "dummy")
        
        # Check counts
        counts = thread_pool.get_unprocessed_counts()
        assert counts["default"] == 2
        assert counts["dummy"] == 1
        assert thread_pool.has_unprocessed_tasks() == True

        # Process all
        results, remaining = thread_pool.process_futures(timeout=0.1)
        
        # Should be empty
        counts = thread_pool.get_unprocessed_counts()
        assert counts["default"] == 0
        assert counts["dummy"] == 0
        assert thread_pool.has_unprocessed_tasks() == False

    def test_shutdown_specific_pool(self):
        """Test shutting down a specific pool."""
        # Create fresh pool for destructive test
        pool = _JobManagerWorkerThreadPool()
        
        # Create two pools
        pool.submit_task(NopTask(job_id="j-1", df_idx=1), "notask")
        pool.submit_task(DummyTask(job_id="j-2", df_idx=2), "dummy")
        
        assert sorted(pool.list_pools()) == ["dummy", "notask"]
        assert pool.has_unprocessed_tasks() == True
        
        # Shutdown notask pool only
        pool.shutdown("notask")
        
        # Only dummy pool should remain
        assert pool.list_pools() == ["dummy"]
        assert pool.has_unprocessed_tasks() == True  # dummy pool still has unprocessed task
        
        # Can submit to existing pool
        pool.submit_task(NopTask(job_id="j-3", df_idx=3), "dummy")
        assert pool.list_pools() == ["dummy"]
        assert pool.get_unprocessed_counts()["dummy"] == 2
        
        pool.shutdown()

    def test_shutdown_all(self):
        """Test shutting down all pools."""
        # Create fresh pool for destructive test
        pool = _JobManagerWorkerThreadPool()
        
        # Create multiple pools
        pool.submit_task(NopTask(job_id="j-1", df_idx=1), "notask")
        pool.submit_task(DummyTask(job_id="j-2", df_idx=2), "dummy")
        
        assert len(pool.list_pools()) == 2
        assert pool.has_unprocessed_tasks() == True
        
        # Shutdown all
        pool.shutdown()

        assert pool.list_pools() == []
        assert len(pool._pools) == 0
        assert pool.has_unprocessed_tasks() == False
        assert pool.get_unprocessed_counts() == {}

    def test_custom_get_pool_name(self):
        """Test custom task class."""
        
        @dataclass(frozen=True)
        class CustomTask(Task):            
            def execute(self) -> _TaskResult:
                return _TaskResult(job_id=self.job_id, df_idx=self.df_idx)
        
        pool = _JobManagerWorkerThreadPool()
        
        task = CustomTask(job_id="j-1", df_idx=1)
        pool.submit_task(task, "custom_pool")
        
        # Pool should be named after class
        assert pool.list_pools() == ["custom_pool"]
        assert pool.get_unprocessed_counts()["custom_pool"] == 1
        assert pool.has_unprocessed_tasks() == True
        
        # Process it
        results, remaining = pool.process_futures(timeout=0.1)
        assert len(results) == 1
        assert results[0].job_id == "j-1"
        assert remaining["custom_pool"] == 0
        assert pool.has_unprocessed_tasks() == False
        assert pool.get_unprocessed_counts()["custom_pool"] == 0
        
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
        assert thread_pool.list_pools() == ["default"]
        assert thread_pool.get_unprocessed_counts()["default"] == 15
        assert thread_pool.has_unprocessed_tasks() == True
        
        # Process them all
        results, remaining = thread_pool.process_futures(timeout=0.5)
        
        assert len(results) == 15
        assert remaining["default"] == 0
        assert thread_pool.get_unprocessed_counts()["default"] == 0
        assert thread_pool.has_unprocessed_tasks() == False

    def test_pool_parallelism_with_blocking_tasks(self):
        """Test that multiple workers allow parallel execution."""
        pool = _JobManagerWorkerThreadPool({
            "BlockingTask": 3,  # 3 workers for blocking tasks
        })
        
        # Create multiple blocking tasks
        events = [threading.Event() for _ in range(5)]
        
        for i, event in enumerate(events):
            pool.submit_task(BlockingTask(
                job_id=f"j-block-{i}", 
                df_idx=i, 
                event=event,
                success=True
            ), "BlockingTask")
        
        # Initially all unprocessed
        assert pool.get_unprocessed_counts()["BlockingTask"] == 5
        assert pool.has_unprocessed_tasks() == True
        
        # Release all events at once
        for event in events:
            event.set()
        
        results, remaining = pool.process_futures(timeout=0.5)        
        assert len(results) == 5
        assert remaining["BlockingTask"] == 0
        assert pool.get_unprocessed_counts()["BlockingTask"] == 0
        assert pool.has_unprocessed_tasks() == False
        
        for result in results:
            assert result.job_id.startswith("j-block-")
        
        pool.shutdown()

    def test_task_with_error_handling(self, thread_pool):
        """Test that task errors are properly handled in the pool."""
        # Submit a failing DummyTask (j-666 fails)
        thread_pool.submit_task(DummyTask(job_id="j-666", df_idx=0))
        
        # Check before processing
        assert thread_pool.has_unprocessed_tasks() == True
        
        # Process it
        results, remaining = thread_pool.process_futures(timeout=0.1)
        
        # Should get error result
        assert len(results) == 1
        result = results[0]
        assert result.job_id == "j-666"
        assert result.db_update == {"status": "threaded task failed"}
        assert result.stats_update == {"threaded task failed": 1}
        assert remaining["default"] == 0
        assert thread_pool.has_unprocessed_tasks() == False

    def test_mixed_success_and_error_tasks(self, thread_pool):
        """Test mix of successful and failing tasks."""
        # Submit mix of tasks
        thread_pool.submit_task(DummyTask(job_id="j-1", df_idx=1))   # Success
        thread_pool.submit_task(DummyTask(job_id="j-666", df_idx=2)) # Failure  
        thread_pool.submit_task(DummyTask(job_id="j-3", df_idx=3))   # Success
        
        # Check before processing
        assert thread_pool.has_unprocessed_tasks() == True
        assert thread_pool.get_unprocessed_counts()["default"] == 3
        
        # Process all
        results, remaining = thread_pool.process_futures(timeout=0.1)
        
        # Should get 3 results
        assert len(results) == 3
        assert remaining["default"] == 0
        assert thread_pool.has_unprocessed_tasks() == False
        
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

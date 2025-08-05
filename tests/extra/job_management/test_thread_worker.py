import logging
import threading
import time
from dataclasses import dataclass
from typing import Iterator

import pytest
import requests

from openeo.extra.job_management._thread_worker import (
    Task,
    _JobManagerWorkerThreadPool,
    _JobStartTask,
    _TaskResult,
)
from openeo.rest._testing import DummyBackend


@pytest.fixture
def dummy_backend(requests_mock) -> DummyBackend:
    dummy = DummyBackend.at_url("https://foo.test", requests_mock=requests_mock)
    dummy.setup_simple_job_status_flow(queued=2, running=5)
    return dummy


class TestTaskResult:
    def test_default(self):
        result = _TaskResult(job_id="j-123", df_idx = 0)
        assert result.job_id == "j-123"
        assert result.df_idx ==0
        assert result.db_update == {}
        assert result.stats_update == {}


class TestJobStartTask:
    def test_start_success(self, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        job = dummy_backend.connection.create_job(process_graph={})

        task = _JobStartTask(job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token="h4ll0")
        result = task.execute()

        assert result == _TaskResult(
            job_id="job-000",
            df_idx = 0,
            db_update={"status": "queued"},
            stats_update={"job start": 1},
        )
        assert job.status() == "queued"
        assert caplog.messages == []

    def test_start_failure(self, dummy_backend, caplog):
        caplog.set_level(logging.WARNING)
        job = dummy_backend.connection.create_job(process_graph={})
        dummy_backend.setup_job_start_failure()

        task = _JobStartTask(job_id=job.job_id, df_idx=0, root_url=dummy_backend.connection.root_url, bearer_token="h4ll0")
        result = task.execute()

        assert result == _TaskResult(
            job_id="job-000",
            df_idx=0,
            db_update={"status": "start_failed"},
            stats_update={"start_job error": 1},
        )
        assert job.status() == "error"
        assert caplog.messages == [
            "Failed to start job 'job-000': OpenEoApiError('[500] Internal: No job starting " "for you, buddy')"
        ]

    @pytest.mark.parametrize("serializer", [repr, str])
    def test_hide_token(self, serializer):
        secret = "Secret!"
        task = _JobStartTask(job_id="job-123", df_idx=0, root_url="https://example.com", bearer_token=secret)
        serialized = serializer(task)
        assert "job-123" in serialized
        assert secret not in serialized




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




class TestJobManagerWorkerThreadPool:
    @pytest.fixture
    def worker_pool(self) -> Iterator[_JobManagerWorkerThreadPool]:
        """Fixture for creating and cleaning up a worker thread pool."""
        pool = _JobManagerWorkerThreadPool(max_workers=2)
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
                df_idx = 0,
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
        assert results == [_TaskResult(job_id="j-22", df_idx=22), _TaskResult(job_id="j-222",  df_idx=222)]
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
            _TaskResult(job_id="j-0", df_idx = 0, db_update={"status": "all fine"}),
        ]
        assert remaining == n - 1

        # Release all but one event
        for j in range(n - 1):
            events[j].set()
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [
            _TaskResult(job_id="j-1", df_idx = 1, db_update={"status": "all fine"}),
            _TaskResult(job_id="j-2", df_idx = 2, db_update={"status": "all fine"}),
            _TaskResult(
                job_id="j-3", df_idx = 3, db_update={"status": "threaded task failed"}, stats_update={"threaded task failed": 1}
            ),
        ]
        assert remaining == 1

        # Release all events
        for j in range(n):
            events[j].set()
        results, remaining = worker_pool.process_futures(timeout=0.1)
        assert results == [
            _TaskResult(job_id="j-4", df_idx = 4, db_update={"status": "all fine"}),
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
                df_idx = 0,
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
            _TaskResult(job_id="job-000", df_idx=0, db_update={"status": "start_failed"}, stats_update={"start_job error": 1})
        ]
        assert remaining == 0
        assert caplog.messages == [
            "Failed to start job 'job-000': OpenEoApiError('[500] Internal: No job starting for you, buddy')"
        ]

import concurrent.futures
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import openeo

_log = logging.getLogger(__name__)


@dataclass
class _TaskResult:
    """
    Container for the result of a task execution.
    Used to communicate the outcome of job-related tasks.

    :param job_id:
        The ID of the job this result is associated with.

    :param db_update:
        Optional dictionary describing updates to apply to a job database,
        such as status changes. Defaults to an empty dict.

    :param stats_update:
        Optional dictionary capturing statistical counters or metrics,
        e.g., number of successful starts or errors. Defaults to an empty dict.
    """

    job_id: str  # Mandatory
    db_update: Dict[str, Any] = field(default_factory=dict)  # Optional
    stats_update: Dict[str, int] = field(default_factory=dict)  # Optional


class Task(ABC):
    """
    Abstract base class for asynchronous tasks.

    A task encapsulates a unit of work, typically executed asynchronously,
    and returns a `_TaskResult` with job-related metadata and updates.

    Implementations must override the `execute` method to define the task logic.
    """

    @abstractmethod
    def execute(self) -> _TaskResult:
        """Execute the task and return a raw result"""
        pass


@dataclass
class _JobStartTask(Task):
    """
    Task for starting a backend job asynchronously.

    Connects to an OpenEO backend using the provided URL and optional token,
    retrieves the specified job, and attempts to start it.

    Usage example:

    .. code-block:: python

        task = _JobStartTask(
            job_id="1234",
            root_url="https://openeo.test",
            bearer_token="secret"
        )
        result = task.execute()

    :param job_id:
        Identifier of the job to start on the backend.

    :param root_url:
        The root URL of the OpenEO backend to connect to.

    :param bearer_token:
        Optional Bearer token used for authentication.

    :raises ValueError:
        If any of the input parameters are invalid (e.g., empty strings).
    """

    job_id: str
    root_url: str
    bearer_token: Optional[str]

    def __post_init__(self) -> None:
        # Validation remains unchanged
        if not isinstance(self.root_url, str) or not self.root_url.strip():
            raise ValueError(f"root_url must be a non-empty string, got {self.root_url!r}")
        if self.bearer_token is not None and (not isinstance(self.bearer_token, str) or not self.bearer_token.strip()):
            raise ValueError(f"bearer_token must be a non-empty string or None, got {self.bearer_token!r}")
        if not isinstance(self.job_id, str) or not self.job_id.strip():
            raise ValueError(f"job_id must be a non-empty string, got {self.job_id!r}")

    def execute(self) -> _TaskResult:
        """
        Executes the job start process using the OpenEO connection.

        Authenticates if a bearer token is provided, retrieves the job by ID,
        and attempts to start it.

        :returns:
            A `_TaskResult` with status and statistics metadata, indicating
            success or failure of the job start.
        """
        try:
            conn = openeo.connect(self.root_url)
            if self.bearer_token:
                conn.authenticate_bearer_token(self.bearer_token)
            job = conn.job(self.job_id)
            job.start()
            _log.info(f"Job {self.job_id} started successfully")
            return _TaskResult(
                job_id=self.job_id,
                db_update={"status": "queued"},
                stats_update={"job start": 1},
            )
        except Exception as e:
            _log.error(f"Failed to start job {self.job_id}: {e}")
            return _TaskResult(
                job_id=self.job_id, db_update={"status": "start_failed"}, stats_update={"start_job error": 1}
            )


class _JobManagerWorkerThreadPool:
    """
    Thread pool-based worker that manages the execution of asynchronous tasks.

    Internally wraps a `ThreadPoolExecutor` and manages submission,
    tracking, and result processing of tasks.

    :param max_workers:
        Maximum number of concurrent threads to use for execution.
        Defaults to 2.
    """

    def __init__(self, max_workers: int = 2):
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._future_task_pairs: List[Tuple[concurrent.futures.Future, Task]] = []

    def submit_task(self, task: Task) -> None:
        """
        Submit a task to the thread pool executor.

        Tasks are scheduled for asynchronous execution and tracked
        internally to allow later processing of their results.

        :param task:
            An instance of `Task` to be executed.
        """
        future = self._executor.submit(task.execute)
        self._future_task_pairs.append((future, task))  # Track pairs

    def process_futures(self) -> List[_TaskResult]:
        """
        Process and retrieve results from completed tasks.

        This method checks which futures have finished without blocking,
        collects their results.

        :returns:
            A list of `_TaskResult` objects from completed tasks.
        """
        results = []
        to_keep = []

        # Use timeout=0 to avoid blocking and check for completed futures
        done, _ = concurrent.futures.wait(
            [f for f, _ in self._future_task_pairs], timeout=0, return_when=concurrent.futures.FIRST_COMPLETED
        )

        # Process completed futures and their tasks
        for future, task in self._future_task_pairs:
            if future in done:
                try:
                    result = future.result()

                except Exception as e:
                    _log.exception(f"Error processing task: {e}")
                    result = _TaskResult(
                        job_id=task.job_id, db_update={"status": "start_failed"}, stats_update={"start_job error": 1}
                    )

                results.append(result)
            else:
                to_keep.append((future, task))

        self._future_task_pairs = to_keep
        return results

    def shutdown(self) -> None:
        """Shuts down the thread pool gracefully."""
        _log.info("Shutting down thread pool")
        self._executor.shutdown(wait=True)

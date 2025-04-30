"""
Internal utilities to handle job management tasks through threads.
"""

import concurrent.futures
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import openeo

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class Task(ABC):
    """
    Abstract base class for a unit of work associated with a job (identified by a job id)
    and to be processed by :py:classs:`_JobManagerWorkerThreadPool`.

    Because the work is intended to be executed in a thread/process pool,
    it is recommended to keep the state of the task object as simple/immutable as possible
    (e.g. just some string/number attributes) and avoid sharing complex objects and state.

    The main API for subclasses to implement is the `execute`method
    which should return a :py:class:`_TaskResult` object.
    with job-related metadata and updates.

    :param job_id:
        Identifier of the job to start on the backend.
    """

    # TODO: strictly speaking, a job id does not unambiguously identify a job when multiple backends are in play.
    job_id: str

    @abstractmethod
    def execute(self) -> _TaskResult:
        """Execute the task and return a raw result"""
        pass


@dataclass(frozen=True)
class ConnectedTask(Task):
    """
    Base class for tasks that involve an (authenticated) connection to a backend.

    Backend is specified by a root URL,
    and (optional) authentication is done through an openEO-style bearer token.

    :param root_url:
        The root URL of the OpenEO backend to connect to.

    :param bearer_token:
        Optional Bearer token used for authentication.

    """

    root_url: str
    bearer_token: Optional[str]

    def get_connection(self) -> openeo.Connection:
        connection = openeo.connect(self.root_url)
        if self.bearer_token:
            connection.authenticate_bearer_token(self.bearer_token)
        return connection


class _JobStartTask(ConnectedTask):
    """
    Task for starting an openEO batch job (the `POST /jobs/<job_id>/result` request).
    """

    def execute(self) -> _TaskResult:
        """
        Start job identified by `job_id` on the backend.

        :returns:
            A `_TaskResult` with status and statistics metadata, indicating
            success or failure of the job start.
        """
        # TODO: move main try-except block to base class?
        try:
            job = self.get_connection().job(self.job_id)
            # TODO: only start when status is "queued"?
            job.start()
            _log.info(f"Job {self.job_id!r} started successfully")
            return _TaskResult(
                job_id=self.job_id,
                db_update={"status": "queued"},
                stats_update={"job start": 1},
            )
        except Exception as e:
            _log.error(f"Failed to start job {self.job_id!r}: {e!r}")
            # TODO: more insights about the failure (e.g. the exception) are just logged, but lost from the result
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
                    _log.exception(f"Failed to get result from future: {e}")
                    result = _TaskResult(
                        job_id=task.job_id,
                        db_update={"status": "future.result() failed"},
                        stats_update={"future.result() error": 1},
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

import concurrent.futures
import logging
import dataclasses
from typing import Optional, Any, List, Dict
import openeo
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Dict


_log = logging.getLogger(__name__)


@dataclass
class _TaskResult():
    """Holds the result of a job start task execution."""
    success: bool
    value: Any


class Task(ABC):
    """Abstract base class for tasks that can be executed asynchronously."""

    @abstractmethod
    def execute(self) -> _TaskResult:
        """Executes the task and returns a result object."""
        pass

    @abstractmethod
    def postprocess(self, result: _TaskResult, context: Dict[str, Any]) -> None:
        """Postprocesses the result using the provided context."""
        pass


@dataclasses.dataclass
class _JobStartTask(Task):
    """
    A task for starting jobs asynchronously.

    Attributes:
        root_url (str): The URL of the backend.
        bearer_token (Optional[str]): An optional token for authentication.
        job_id (str): The identifier of the job to start.
    """
    root_url: str
    bearer_token: Optional[str]
    job_id: str

    def __post_init__(self) -> None:
        # Validation remains unchanged
        if not isinstance(self.root_url, str) or not self.root_url.strip():
            raise ValueError(f"root_url must be a non-empty string, got {self.root_url!r}")
        if self.bearer_token is not None and (not isinstance(self.bearer_token, str) or not self.bearer_token.strip()):
            raise ValueError(f"bearer_token must be a non-empty string or None, got {self.bearer_token!r}")
        if not isinstance(self.job_id, str) or not self.job_id.strip():
            raise ValueError(f"job_id must be a non-empty string, got {self.job_id!r}")

    def execute(self) -> _TaskResult:
        """Executes the job start task and returns a JobStartResult."""
        try:
            conn = openeo.connect(self.root_url)
            if self.bearer_token:
                conn.authenticate_bearer_token(self.bearer_token)
            job = conn.job(self.job_id)
            job.start()
            return _TaskResult(success=True, value=job.status())
        except Exception as e:
            return _TaskResult(success=False, value=str(e))
        

    #TODO can we avoid parsing this entire dataframe....
    def postprocess(self, result: _TaskResult, context: Dict[str, Any]) -> None:
        """Updates the DataFrame and statistics based on the job start result."""
        df = context.get("df")
        stats = context.get("stats")
        if df is None or stats is None:
            _log.error("Context must include 'df' and 'stats'")
            return

        # Use self.job_id to find the relevant row
        idx = df.index[(df["id"] == self.job_id) & (df["status"] == "queued_for_start")]
        if not idx.empty:
            new_status = "queued" if result.success else "start_failed"
            df.loc[idx, "status"] = new_status
            _log.info(f"Updated job {self.job_id} status to {new_status} in dataframe.")
        else:
            _log.warning(f"No entry for job {self.job_id} with status 'queued_for_start' found, passing.")

        # Update statistics based on result
        if result.success:
            _log.info(f"Job {self.job_id} started successfully. Status: {result.value}")
            stats["job start"] += 1
        else:
            _log.warning(f"Job {self.job_id} start failed. Error: {result.value}")
            stats["job start failed"] += 1


class _JobManagerWorkerThreadPool:
    """
    Manages a thread pool for executing tasks asynchronously and handles postprocessing.
    """
    def __init__(self, max_workers: int = 2):
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._future_task_pairs: List[Tuple[concurrent.futures.Future, Task]] = []

    def submit_task(self, task: Task) -> None:
        future = self._executor.submit(task.execute)
        self._future_task_pairs.append((future, task))  # Track pairs

    def process_futures(self, context: Dict[str, Any]) -> None:
        done, _ = concurrent.futures.wait(
            [f for f, _ in self._future_task_pairs], timeout=0,
            return_when=concurrent.futures.FIRST_COMPLETED
        )

        # Process completed futures and their tasks
        for future, task in self._future_task_pairs[:]:
            if future in done:
                try:
                    result = future.result()
                    task.postprocess(result, context)
                except Exception as e:
                    _log.exception(f"Error processing task: {e}")
                finally:
                    self._future_task_pairs.remove((future, task))  # Cleanup

    def shutdown(self) -> None:
        """Shuts down the thread pool gracefully."""
        _log.info("Shutting down thread pool")
        self._executor.shutdown(wait=True)

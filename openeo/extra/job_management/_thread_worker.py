
import concurrent.futures
import logging
import dataclasses
from typing import Optional, Any, Tuple, List
import openeo  

_log = logging.getLogger(__name__)

# Define a generic TaskResult to encapsulate success and a resulting value or error.
@dataclasses.dataclass
class _TaskResult:
    success: bool
    value: Any  # e.g., status message or error message

# Each task can implement its own postprocessing logic.
@dataclasses.dataclass
class _JobStartTask:
    root_url: str
    bearer_token: Optional[str]
    job_id: str

    def __post_init__(self):
        if not isinstance(self.root_url, str) or not self.root_url.strip():
            raise ValueError(f"root_url must be a non-empty string, got {self.root_url!r}")
        if self.bearer_token is not None and (not isinstance(self.bearer_token, str) or not self.bearer_token.strip()):
            raise ValueError(f"bearer_token must be a non-empty string or None, got {self.bearer_token!r}")
        if not isinstance(self.job_id, str) or not self.job_id.strip():
            raise ValueError(f"job_id must be a non-empty string, got {self.job_id!r}")

    def execute(self) -> _TaskResult:
        try:
            conn = openeo.connect(self.root_url)
            if self.bearer_token:
                conn.authenticate_bearer_token(self.bearer_token)
            job = conn.job(self.job_id)
            job.start()
            return _TaskResult(success=True, value=job.status())
        except Exception as e:
            return _TaskResult(success=False, value=str(e))

    def postprocess(self, df, stats, result: _TaskResult) -> None:
        """
        Postprocess the result specifically for a job start task.
        Updates the dataframe and statistics accordingly.
        """
        # Only consider rows that are in 'queued_for_start' state
        idx = df.index[(df["id"] == self.job_id) & (df["status"] == "queued_for_start")]
        if not idx.empty:
            new_status = "queued" if result.success else "start_failed"
            df.loc[idx, "status"] = new_status
            _log.info(f"Updated job {self.job_id} status to {new_status} in dataframe.")
        else:
            _log.warning(f"No entry for job {self.job_id} with status 'queued_for_start' found in dataframe, passing on.")
        
        # Update task-specific stats
        if result.success:
            _log.info(f"Job {self.job_id} started successfully with status: {result.value}")
            stats["job start"] = stats.get("job start", 0) + 1
        else:
            _log.info(f"Job {self.job_id} start failed with exception: {result.value}")
            stats["job start failed"] = stats.get("job start failed", 0) + 1

class _JobManagerWorkerThreadPool:
    """
    A worker thread pool for processing job management tasks asynchronously.
    """
    def __init__(self, max_workers: int = 2):
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._futures: List[concurrent.futures.Future] = []
        self._task_map = {}  # Maps futures to their corresponding tasks

    def submit_task(self, task: _JobStartTask) -> None:
        _log.info(f"Submitting task for job {task.job_id} to thread pool")
        future = self._executor.submit(task.execute)
        self._futures.append(future)
        self._task_map[future] = task  # Associate the task with its future

    def process_futures(self) -> List[Tuple[Any, _TaskResult]]:
        """
        Processes completed futures and returns a list of (task, result) tuples.
        Delegates postprocessing to each task, so the generic loop remains decoupled from specific logic.
        """
        if not self._futures:
            return []

        done, _ = concurrent.futures.wait(
            self._futures, timeout=0, return_when=concurrent.futures.FIRST_COMPLETED
        )

        results = []
        for future in done:
            task = self._task_map.pop(future, None)
            try:
                result = future.result()
                results.append((task, result))
            except Exception as e:
                _log.exception(f"Unexpected error processing future: {e}")
                results.append((task, _TaskResult(success=False, value=str(e))))
            self._futures.remove(future)
        return results

    def shutdown(self):
        _log.info("Shutting down worker thread pool")
        if self._futures:
            _log.warning(f"Shutting down with {len(self._futures)} unprocessed futures")
        self._executor.shutdown(wait=True)

def _postprocess_futures(worker_pool_results, df, stats):
    """
    Processes completed tasks from the worker pool and updates the job DataFrame and statistics.
    For each task, it checks the type and performs task-specific postprocessing.
    """
    for task, result in worker_pool_results:

        if isinstance(task, _JobStartTask):
            # Look for rows with matching job id and a status of 'queued_for_start'
            idx = df.index[(df["id"] == task.job_id) & (df["status"] == "queued_for_start")]
            if not idx.empty:
                # Update the job's status based on whether the task succeeded
                new_status = "queued" if result.success else "start_failed"
                df.loc[idx, "status"] = new_status
                _log.info(f"Updated job {task.job_id} status to {new_status} in dataframe.")
            else:
                _log.warning(f"No entry for job {task.job_id} with status 'queued_for_start' found in dataframe, passing on.")

            # Update statistics based on task result
            if result.success:
                _log.info(f"Job {task.job_id} started successfully with status: {result.value}")
                stats["job start"] += 1
            else:
                _log.info(f"Job {task.job_id} start failed with exception: {result.value}")
                stats["job start failed"] += 1

        # Fallback generic processing for tasks that are not explicitly handled
        else:
            pass

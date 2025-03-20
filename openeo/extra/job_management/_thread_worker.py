#%%

import openeo
import dataclasses

import logging
import concurrent.futures
from typing import Tuple, Optional
from typing import Any, List, Tuple


_log = logging.getLogger(__name__)

@dataclasses.dataclass
class _JobStartTask:
    root_url: str
    bearer_token: Optional[str]
    job_id: str

    def __post_init__(self):
        """Validates task parameters upon initialization."""
        if not isinstance(self.root_url, str) or not self.root_url.strip():
            raise ValueError(f"root_url must be a non-empty string, got {self.root_url!r}")
        if self.bearer_token is not None and (not isinstance(self.bearer_token, str) or not self.bearer_token.strip()):
            raise ValueError(f"bearer_token must be a non-empty string or None, got {self.bearer_token!r}")
        if not isinstance(self.job_id, str) or not self.job_id.strip():
            raise ValueError(f"job_id must be a non-empty string, got {self.job_id!r}")

    def execute(self) -> Tuple[str, bool, str]:
        """Executes the job start task and returns the result."""
        try:
            conn = openeo.connect(self.root_url)
            if self.bearer_token:
                conn.authenticate_bearer_token(self.bearer_token)
            job = conn.job(self.job_id)
            job.start()
            status = job.status()
            return (self.job_id, True, status)
        except Exception as e:
            return (self.job_id, False, str(e))

class _JobManagerWorkerThreadPool:
    """
    A worker thread pool for processing job management tasks asynchronously.

    This thread pool is designed to handle tasks such as starting jobs on a backend.
    It uses a `ThreadPoolExecutor` to process tasks concurrently and tracks their results
    using futures.


    :param _executor: (concurrent.futures.ThreadPoolExecutor): The thread pool executor used to manage worker threads.
    :param _futures: (List[concurrent.futures.Future, str]): A list of futures.
    """

    def __init__(self, max_workers: int = 2):
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._futures: List[concurrent.futures.Future] = []

    def submit_task(self, task) -> None:
        """Submits a task to the thread pool."""
        _log.info(f"Submitting task {task} to thread pool")
        future = self._executor.submit(task.execute)
        future.task = task
        self._futures.append(future)

    def process_futures(self) -> List[Tuple]:
        """Processes completed futures and returns tuples of (task, result)."""
        if not self._futures:
            return []

        done, _ = concurrent.futures.wait(
            self._futures,
            timeout=0,
            return_when=concurrent.futures.FIRST_COMPLETED,
        )

        results = []
        for future in done:
            try:
                result = future.result()
                results.append((future.task, result))  # Return the task and its result
            except Exception as e:
                _log.exception(f"Unexpected error processing future: {e}")
                results.append((future.task, str(e)))
            self._futures.remove(future)

        return results

    def shutdown(self):
        """Shuts down the thread pool, warning about unprocessed futures."""
        _log.info("Shutting down worker thread pool")
        if self._futures:
            _log.warning(f"Shutting down with {len(self._futures)} unprocessed futures")
        self._executor.shutdown(wait=True)


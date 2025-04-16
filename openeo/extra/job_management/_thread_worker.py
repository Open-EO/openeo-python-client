import concurrent.futures
import logging
from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict, Tuple
import openeo
from abc import ABC, abstractmethod


_log = logging.getLogger(__name__)

@dataclass
class _TaskResult:
    """Container for task results with optional components"""
    job_id: str  # Mandatory
    db_update: Dict[str, Any] = field(default_factory=dict)  # Optional
    stats_update: Dict[str, int] = field(default_factory=dict)  # Optional

class Task(ABC):
    """Abstract base class for asynchronous tasks with safe update generation"""
    
    @abstractmethod
    def execute(self) -> _TaskResult:
        """Execute the task and return a raw result"""
        pass
    
@dataclass
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
            _log.info(f"Job {self.job_id} started successfully")
            return _TaskResult(
                job_id=self.job_id,
                db_update={"status": "queued"},
                stats_update={"job start": 1},
            )
        except Exception as e:
            _log.error(f"Failed to start job {self.job_id}: {e}")
            return _TaskResult(
                job_id=self.job_id,
                db_update={"status": "start_failed"},  
                stats_update={"start_job error": 1})
        
class _JobManagerWorkerThreadPool:
    """
    Manages a thread pool for executing tasks asynchronously and handles postprocessing.
    """
    def __init__(self, max_workers: int = 2):
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._future_task_pairs: List[Tuple[concurrent.futures.Future, Task]] = []

    def submit_task(self, task: Task) -> None:
        """
        ubmits a tasks to the internal Threadpool executor and keeps.
        """
        future = self._executor.submit(task.execute)
        self._future_task_pairs.append((future, task))  # Track pairs

    def process_futures(self) -> list[ _TaskResult]:

        results = []  
        to_keep = [] 

        # Use timeout=0 to avoid blocking and check for completed futures
        done, _ = concurrent.futures.wait(
            [f for f, _ in self._future_task_pairs], timeout=0,
            return_when=concurrent.futures.FIRST_COMPLETED
        )

        # Process completed futures and their tasks
        for future, task in self._future_task_pairs:
            if future in done:
                try:
                    result = future.result()
                    results.append(result)
                    
                except Exception as e:
                    _log.exception(f"Error processing task: {e}")
            else:  
                to_keep.append((future, task))  

        self._future_task_pairs = to_keep  
        return results

    def shutdown(self) -> None:
        """Shuts down the thread pool gracefully."""
        _log.info("Shutting down thread pool")
        self._executor.shutdown(wait=True)

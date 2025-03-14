
from  typing import Tuple, Any, Dict
import logging
import openeo
import concurrent.futures

_log = logging.getLogger(__name__)

class _JobManagerWorkerThreadPool:
    """
    Worker threadpool for processing job management tasks.

    This threadpool continuously polls a work queue for tasks (e.g. starting jobs on a backend).
    It processes each task concurrently using a ThreadPoolExecutor and reports the results (or any
    encountered errors) to a result queue. This implementation is intended for use within a job
    management system that coordinates asynchronous job start requests.

    Attributes:
        WORK_TYPE_START_JOB (str): Constant indicating a "start job" work item.
        
        executor (concurrent.futures.ThreadPoolExecutor): Thread pool with a maximum of 4 worker threads.
       
    """

    WORK_TYPE_START_JOB = "start_job"

    def __init__(self, max_workers: int = 2):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.futures: Dict[concurrent.futures.Future, str] = {}

    def submit_work(self, work_type: str, work_args: Tuple[Any, ...]) -> None:
        """Submit work to the executor and track its Future."""
        job_id = work_args[2] if len(work_args) > 2 else "unknown"
        _log.info(f"Submitting {work_type} for job {job_id}")
        future = self.executor.submit(self._process_work_item, work_type, work_args)
        self.futures[future] = job_id

    def _process_work_item(self, work_type: str, work_args: Tuple[Any, ...]) -> Tuple[str, bool, str]:
        """Process a work item and return (job_id, success, data)."""
        job_id = work_args[2] if len(work_args) > 2 else "unknown"
        try:
            if work_type == self.WORK_TYPE_START_JOB:
                root_url, bearer, job_id = work_args
                conn = openeo.connect(root_url)
                if bearer:
                    conn.authenticate_bearer_token(bearer)
                job = conn.job(job_id)
                job.start()
                status = job.status()
                _log.info(f"Job {job_id} started successfully. Status: {status}")
                return (job_id, True, status)
            else:
                raise ValueError(f"Unknown work type: {work_type}")
        except Exception as e:
            _log.exception(f"Job {job_id} failed: {e}")
            return (job_id, False, str(e))

    def process_futures(self, stats: Dict[str, int]) -> None:
        """Process completed futures and update stats."""
        completed = []
        _log.info(f"Processing {len(self.futures)} futures")
        for future in concurrent.futures.as_completed(self.futures):
            job_id = self.futures[future]
            try:
                job_id, success, data = future.result()
                if success:
                    stats["job start"] += 1
                else:
                    stats["job start failed"] += 1
                    _log.error(f"Job {job_id} failed: {data}")
            except Exception as e:
                _log.exception(f"Unexpected error processing job {job_id}: {e}")
            completed.append(future)
        
        # Remove processed futures
        for future in completed:
            del self.futures[future]
        
        _log.info(f"Processed {len(completed)} jobs")

    def shutdown(self):
        """Clean shutdown of the executor."""
        _log.info("Shutting down worker thread pool")
        self.executor.shutdown(wait=True)
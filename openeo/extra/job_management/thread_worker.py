
from  typing import Tuple, Any, List, Dict
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
        self.futures: List[concurrent.futures.Future] = []

    def submit_work(self, work_type: str, work_args: Tuple[Any, ...]) -> None:
        """Submit work to the executor and track its Future."""
        _log.info(f"Submitting {work_type} for job {work_args[2]}")
        future = self.executor.submit(self._process_work_item, work_type, work_args)
        self.futures.append(future)

    def _process_work_item(self, work_type: str, work_args: Tuple[Any, ...]) -> Tuple[str, bool, str]:
        """Process a work item and return (job_id, success, data)."""
        try:
            if work_type == self.WORK_TYPE_START_JOB:
                root_url, bearer, job_id = work_args
                conn = openeo.connect(root_url)
                if bearer:
                    conn.authenticate_bearer_token(bearer)
                job = conn.job(job_id)
                job.start()
                status = job.status()
                _log.info(f"Job {job_id} started. Status: {status}")
                return (job_id, True, status)
            else:
                raise ValueError(f"Unknown work type: {work_type}")
        except Exception as e:
            error_msg = f"Job {job_id} failed: {str(e)}"
            _log.error(error_msg)
            return (job_id, False, error_msg)

    def process_futures(self, stats: Dict[str, int]) -> None:
        """Process completed futures and update stats."""
        completed = []
        _log.info(f"Processing {len(self.futures)} futures")
        for future in concurrent.futures.as_completed(self.futures):
            try:
                job_id, success, data = future.result()
                if success:
                    stats["job start"] += 1
                else:
                    stats["job start failed"] += 1
                    _log.info(f"Job {job_id} failed: {data}")
            except Exception as e:
                _log.error(f"Error processing future: {e}")
            completed.append(future)
        
        # Remove processed futures
        _log.info(f"Processed {len(completed)} futures")
        for future in completed:
            self.futures.remove(future)

    def shutdown(self):
        """Clean shutdown of the executor."""
        self.executor.shutdown(wait=True)
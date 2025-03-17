
from  typing import Tuple, Any, List, Dict
import logging
import openeo
import concurrent.futures

_log = logging.getLogger(__name__)

class _JobManagerWorkerThreadPool:
    """
    A worker thread pool for processing job management tasks asynchronously.

    This thread pool is designed to handle tasks such as starting jobs on a backend.
    It uses a `ThreadPoolExecutor` to process tasks concurrently and tracks their results
    using futures.

    Attributes:
        Public:
            WORK_TYPE_START_JOB (str): A constant representing the work type (e.g. starting jobs).
        Private:
            _executor (concurrent.futures.ThreadPoolExecutor): The thread pool executor used to manage worker threads.
            _futures (List[concurrent.futures.Future, str]): A list of futures.
    """

    WORK_TYPE_START_JOB = "start_job"

    def __init__(self, max_workers: int = 2):
        """
        Initialize the worker thread pool.

        Args:
            max_workers (int): The maximum number of worker threads to use in the thread pool.
                              Defaults to 2.
        """
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._futures: List[concurrent.futures.Future] = []

    def submit_work(self, work_type: str, work_args: Tuple[Any, ...]) -> None:
        """
        Submit a work item to the thread pool for processing.

        Args:
            work_type (str): The type of work to be performed.
            work_args (Tuple[Any, ...]): A tuple of arguments required to process the work item.
        """
        # Validate work type and arguments
        self._validate_work_args(work_type, work_args)

        # Log the submission
        _log.info(f"Submitting {work_type} for job {work_args[2]}")

        # Submit the work item to the thread pool
        future = self._executor.submit(self._process_work_item, work_type, work_args)
        self._futures.append(future)


    def _process_work_item(self, work_type: str, work_args: Tuple[Any, ...]) -> Tuple[str, bool, str]:
        """
        Process a work item and return its result.

        Args:
            work_type (str): The type of work to be performed (e.g., `WORK_TYPE_START_JOB`).
            work_args (Tuple[Any, ...]): A tuple of arguments required to process the work item.
                        The first two elements are expected to be the root URL and bearer token, respectively.
                        The third element is expected to be the job ID.

        Returns:
            Tuple[str, bool, str]: A tuple containing:
                - The job ID.
                - A boolean indicating job_start success (`True`) or failure (`False`).
                - Additional data (e.g., job status or error message).

        Raises:
            ValueError: If the work type is unknown.
        """

        root_url, bearer, job_id = work_args
        
        try:
            if work_type == self.WORK_TYPE_START_JOB:
                
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
        """
        Process completed futures and update statistics.
        Processed futures are removed from the internal tracking dictionary

        Args:
            stats (Dict[str, int]): A dictionary to track job processing statistics.
                                   Keys represent statistic names (e.g., "job start"),
                                   and values represent their counts.

        """

        if not self._futures:
            return # no futures to process
        
        done, _ = concurrent.futures.wait(
                    self._futures,
                    timeout=0,
                    return_when=concurrent.futures.FIRST_COMPLETED)

        completed = []

        _log.info(f"Processing {len(self._futures)} futures")
        for future in done:
            try:
                job_id, success, data = future.result()
                if success:
                    stats["job start"] += 1
                else:
                    stats["job start failed"] += 1
                    _log.error(f"Job {job_id} failed: {data}")
            except Exception as e:
                _log.exception(f"Unexpected error processing future: {e}")
            completed.append(future)
        
        _log.info(f"Processed {len(completed)} jobs")
        # Remove processed futures
        for future in completed:
            self._futures.remove(future)

    def _validate_work_args(self, work_type: str, work_args: Tuple[Any, ...]) -> None:
        """
        Validate the work type and arguments before submitting work to the thread pool.

        Args:
            work_type (str): The type of work to be performed.
            work_args (Tuple[Any, ...]): A tuple of arguments required to process the work item.

        Raises:
            ValueError: If the work type is unsupported or the number of arguments is incorrect.
            TypeError: If the argument types are invalid.
        """
        if work_type != self.WORK_TYPE_START_JOB:
            error_msg = f"Unsupported work type: {work_type}"
            _log.error(error_msg)
            raise ValueError(error_msg)

        if len(work_args) != 3:
            error_msg = f"Expected 3 arguments for work type {work_type}, got {len(work_args)}"
            _log.error(error_msg)
            raise ValueError(error_msg)

        root_url, bearer, job_id = work_args

        if not isinstance(root_url, str):
            error_msg = f"root_url must be a string, got {root_url}"
            _log.error(error_msg)
            raise TypeError(error_msg)
        if bearer is not None and not isinstance(bearer, str):
            error_msg = f"bearer must be a string or None, got {bearer}"
            _log.error(error_msg)
            raise TypeError(error_msg)
        if not isinstance(job_id, str):
            error_msg = f"job_id must be a string, got {job_id}"
            _log.error(error_msg)
            raise TypeError(error_msg)
        
    def shutdown(self):
        """
        Shut down the thread pool gracefully.

        Notes:
            - This method ensures that all worker threads are terminated.
            - It waits for all pending tasks to complete before shutting down.
        """
        _log.info("Shutting down worker thread pool")
        self._executor.shutdown(wait=True)
import threading
import queue
import time
import logging
import openeo

_log = logging.getLogger(__name__)


class _JobManagerWorkerThread(threading.Thread):
    """
    Worker thread for processing job management tasks.

    This thread continuously polls a work queue for tasks such as starting jobs on a
    backend. It processes each task and reports the result (or any error encountered)
    to a result queue.

    Attributes:
        WORK_TYPE_START_JOB (str): Constant indicating a "start job" work item.
        work_queue (queue.Queue): Queue from which work items are fetched.
        result_queue (queue.Queue): Queue to which task results are pushed.
        stop_event (threading.Event): Event flag to signal the thread to stop processing.
        polling_time (int): Interval (in seconds) for polling the work queue.
    """
    WORK_TYPE_START_JOB = "start_job"

    def __init__(self, work_queue: queue.Queue, result_queue: queue.Queue) -> None:
        """
        Initialize the JobManagerWorkerThread.

        Args:
            work_queue (queue.Queue): Queue for incoming work tasks.
            result_queue (queue.Queue): Queue for outgoing results.
        """
        super().__init__(daemon=True)
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.stop_event = threading.Event()
        self.polling_time = 5  # Seconds to wait for a new task before retrying.
        # TODO: Consider adding customization options for timeout and sleep durations.

    def run(self) -> None:
        """
        Run the worker thread.

        This method polls the work queue at regular intervals and processes tasks.
        If a task has an unknown type or an exception occurs during processing, the
        error is logged and a failure result is placed in the result queue.
        """
        _log.info("Worker thread started, waiting for tasks")
        while not self.stop_event.is_set():
            try:
                work_type, work_args = self.work_queue.get(timeout=self.polling_time)
                _log.info(f"Received task: {work_type} with args: {work_args}")
                if work_type == self.WORK_TYPE_START_JOB:
                    self._start_job(work_args)
                else:
                    raise ValueError(f"Unknown work item: {work_type!r}")
            except queue.Empty:
                _log.info("No tasks for worker thread, sleeping")
                time.sleep(self.polling_time)
            except Exception as e:
                self.result_queue.put((None, (None, False, repr(e))))
                _log.error(f"Error in worker thread: {e}")

    def _start_job(self, work_args: tuple) -> None:
        """
        Start a job based on the provided work arguments.

        Connects to the specified backend, authenticates if a bearer token is provided,
        and attempts to start the job. The job's status is then retrieved and a success
        result is pushed to the result queue. In case of failure, an error result is pushed.

        Args:
            work_args (tuple): A tuple containing:
                - root_url (str): URL of the backend.
                - bearer (Optional[str]): Bearer token for authentication (if provided).
                - job_id (str): Identifier of the job to start.
        """
        root_url, bearer, job_id = work_args
        _log.info(f"Starting job {job_id} on backend: {root_url}")
        try:
            connection = openeo.connect(url=root_url)
            if bearer:
                _log.info(f"Authenticating with bearer token for job {job_id}")
                connection.authenticate_bearer_token(bearer_token=bearer)

            job = connection.job(job_id)
            job.start()
            status = job.status()
            _log.info(f"Job {job_id} started successfully. Status: {status}")
        except Exception as e:
            self.result_queue.put((self.WORK_TYPE_START_JOB, (job_id, False, repr(e))))
            _log.error(f"Error while starting job {job_id}: {e}")
        else:
            self.result_queue.put((self.WORK_TYPE_START_JOB, (job_id, True, status))) 



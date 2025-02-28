import threading
import queue
import time
import logging
import openeo
import concurrent

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
        work_queue (queue.Queue): Queue from which work items are fetched.
        result_queue (queue.Queue): Queue to which task results are pushed.
        executor (concurrent.futures.ThreadPoolExecutor): Thread pool with a maximum of 4 worker threads.
        stop_event (threading.Event): Event flag used to signal the threadpool to stop processing tasks.
        polling_time (int): Time interval (in seconds) for polling the work queue.
        producer_thread (threading.Thread): Dedicated thread that monitors the work queue and submits tasks
                                            to the executor.
    """

    WORK_TYPE_START_JOB = "start_job"

    def __init__(self, work_queue: queue.Queue, result_queue: queue.Queue, max_worker: int = 2):
        """
        Initialize the JobManagerWorkerThreadPool.

        Args:
            work_queue (queue.Queue): The queue from which work items are fetched.
            result_queue (queue.Queue): The queue to which task results will be pushed.
        """
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_worker)
        self.stop_event = threading.Event()
        self.polling_time = 1
        self.producer_thread = threading.Thread(target=self._producer_loop, daemon=True)

    def start(self):
        """
        Start the worker threadpool.

        This method starts the internal producer thread which continuously monitors the work queue
        and submits tasks to the thread pool for execution.
        """
        self.producer_thread.start()
        _log.info("JobManager started with thread pool")

    def is_alive(self):
        """
        Check if the producer thread is currently running.

        Returns:
            bool: True if the producer thread is alive, False otherwise.
        """
        return self.producer_thread.is_alive()

    def shutdown(self, timeout=None):
        """
        Gracefully shut down the worker threadpool.

        This method signals the producer thread to stop, waits for it to finish, and then shuts down
        the ThreadPoolExecutor. The shutdown process waits for all submitted tasks to complete.

        Args:
            timeout (float, optional): Maximum time in seconds to wait for the producer thread to finish.
                                       If None, it will wait indefinitely.
        """
        _log.info("Initiating shutdown...")
        self.stop_event.set()
        self.producer_thread.join(timeout)
        self.executor.shutdown(wait=True)
        _log.info("Shutdown complete")

    def _producer_loop(self):
        """
        Continuously poll the work queue and submit tasks to the thread pool.

        This internal loop runs in the dedicated producer thread. It attempts to retrieve a work item
        from the work queue at regular intervals defined by `polling_time`. When a work item is obtained,
        it is submitted to the executor for processing. If no work item is available (raising queue.Empty),
        the loop continues. The loop terminates when the `stop_event` is set.
        """
        _log.info("Producer thread started")
        while not self.stop_event.is_set():
            try:
                work_item = self.work_queue.get(timeout=self.polling_time)
                _log.debug(f"Submitting work item: {work_item[0]}")

                # TODO: Consider using the internal queue of the ThreadPoolExecutor,
                # eliminating the need for the producer thread.
                future = self.executor.submit(self._process_work_item, work_item)
                future.add_done_callback(lambda f: self.work_queue.task_done())
            except queue.Empty:
                continue
            except Exception as e:
                _log.error(f"Error submitting work item: {e}", exc_info=True)
                if self.stop_event.is_set():
                    break
        _log.info("Producer thread exiting")

    def _process_work_item(self, work_item):
        """
        Process a single work item from the work queue.

        Args:
            work_item (tuple): A tuple containing the work type and the work arguments.
                               Expected format: (work_type, work_args).

        Raises:
            ValueError: If the work_type is not recognized.

        Any exceptions raised during processing are caught, logged, and an error result is pushed
        to the result queue.
        """
        work_type, work_args = work_item
        try:
            if work_type == self.WORK_TYPE_START_JOB:
                self._start_job(work_args)
            else:
                raise ValueError(f"Unknown work item: {work_type}")
        except Exception as e:
            _log.error(f"Error processing {work_type}: {e}", exc_info=True)
            self.result_queue.put((work_type, (None, False, str(e))))

    def _start_job(self, work_args):
        """
        Handle the job startup logic in a thread-safe manner.

        This method processes a "start job" task by connecting to the specified backend,
        optionally authenticating with a bearer token, retrieving the job by its identifier,
        starting the job, and then fetching the job's status. The result (success or failure) is
        pushed to the result queue.

        Args:
            work_args (tuple): A tuple containing the following:
                - root_url (str): The URL of the backend.
                - bearer (str or None): Bearer token for authentication, if provided.
                - job_id (str): Identifier of the job to be started.
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

import collections
import contextlib
import dataclasses
import datetime
import json
import logging
import time
import warnings
from pathlib import Path
from threading import Thread
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# TODO avoid this (circular) dependency on _job_db?
import openeo.extra.job_management._job_db
from openeo import BatchJob, Connection
from openeo.extra.job_management._interface import JobDatabaseInterface
from openeo.extra.job_management._thread_worker import (
    _JobManagerWorkerThreadPool,
    _JobStartTask,
)
from openeo.rest import OpenEoApiError
from openeo.rest.auth.auth import BearerAuth
from openeo.util import deep_get, rfc3339

_log = logging.getLogger(__name__)


# TODO: eliminate this module constant (should be part of some constructor interface)
MAX_RETRIES = 50


# Sentinel value to indicate that a parameter was not set
_UNSET = object()


def _start_job_default(row: pd.Series, connection: Connection, *args, **kwargs):
    raise NotImplementedError("No 'start_job' callable provided")


class _Backend(NamedTuple):
    """Container for backend info/settings"""

    # callable to create a backend connection
    get_connection: Callable[[], Connection]
    # Maximum number of jobs to allow in parallel on a backend
    parallel_jobs: int

    # Maximum number of jobs to allow in queue on a backend
    queueing_limit: int = 10


@dataclasses.dataclass(frozen=True)
class _ColumnProperties:
    """Expected/required properties of a column in the job manager related dataframes"""

    dtype: str = "object"
    default: Any = None


class _ColumnRequirements:
    """
    Helper class to encapsulate the column requirements expected by MultiBackendJobManager.
    The current implementation (e.g. _job_db) has some undesired coupling here,
    but it turns out quite hard to eliminate.
    The goal of this class is, currently, to at least make the coupling explicit
    in a centralized way.
    """

    def __init__(self, requirements: Mapping[str, _ColumnProperties]):
        self._requirements = dict(requirements)

    def normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize given pandas dataframe (creating a new one):
        ensure we have the required columns.

        :param df: The dataframe to normalize.
        :return: a new dataframe that is normalized.
        """
        new_columns = {col: req.default for (col, req) in self._requirements.items() if col not in df.columns}
        df = df.assign(**new_columns)
        return df

    def dtype_mapping(self) -> Dict[str, str]:
        """
        Get mapping of column name to expected dtype string, e.g. to be used with pandas.read_csv(dtype=...)
        """
        return {col: req.dtype for (col, req) in self._requirements.items()}


class MultiBackendJobManager:
    """
    Tracker for multiple jobs on multiple backends.

    Usage example:

    .. code-block:: python

        import logging
        import pandas as pd
        import openeo
        from openeo.extra.job_management import MultiBackendJobManager

        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )

        manager = MultiBackendJobManager()
        manager.add_backend("foo", connection=openeo.connect("http://foo.test"))
        manager.add_backend("bar", connection=openeo.connect("http://bar.test"))

        jobs_df = pd.DataFrame(...)
        output_file = "jobs.csv"

        def start_job(
            row: pd.Series,
            connection: openeo.Connection,
            **kwargs
        ) -> openeo.BatchJob:
            year = row["year"]
            cube = connection.load_collection(
                ...,
                temporal_extent=[f"{year}-01-01", f"{year+1}-01-01"],
            )
            ...
            return cube.create_job(...)

        manager.run_jobs(df=jobs_df, start_job=start_job, output_file=output_file)

    See :py:meth:`.run_jobs` for more information on the ``start_job`` callable.

    :param poll_sleep:
        How many seconds to sleep between polls.

    :param root_dir:
        Root directory to save files for the jobs, e.g. metadata and error logs.
        This defaults to "." the current directory.

        Each job gets its own subfolder in this root directory.
        You can use the following methods to find the relevant paths,
        based on the job ID:

            - get_job_dir
            - get_error_log_path
            - get_job_metadata_path

    :param download_results:
        Whether to download job results automatically once the job is completed.

    :param cancel_running_job_after:
        Optional temporal limit (in seconds) after which running jobs should be canceled
        by the job manager.


    .. versionadded:: 0.14.0

    .. versionchanged:: 0.32.0
        Added ``cancel_running_job_after`` parameter.

    .. versionchanged:: 0.47.0
        Added ``download_results`` parameter.
    """

    # Expected columns in the job DB dataframes.
    # TODO: make this part of public API when settled?
    # TODO: move non official statuses to separate column (not_started, queued_for_start)
    _column_requirements: _ColumnRequirements = _ColumnRequirements(
        {
            "id": _ColumnProperties(dtype="str"),
            "backend_name": _ColumnProperties(dtype="str"),
            "status": _ColumnProperties(dtype="str", default="not_started"),
            # TODO: use proper date/time dtype instead of legacy str for start times?
            "start_time": _ColumnProperties(dtype="str"),
            "running_start_time": _ColumnProperties(dtype="str"),
            # TODO: these columns "cpu", "memory", "duration" are not referenced explicitly from MultiBackendJobManager,
            #       but are indirectly coupled through handling of VITO-specific "usage" metadata in `_track_statuses`.
            #       Since bfd99e34 they are not really required to be present anymore, can we make that more explicit?
            "cpu": _ColumnProperties(dtype="str"),
            "memory": _ColumnProperties(dtype="str"),
            "duration": _ColumnProperties(dtype="str"),
            "costs": _ColumnProperties(dtype="float64"),
        }
    )

    def __init__(
        self,
        poll_sleep: int = 60,
        root_dir: Optional[Union[str, Path]] = ".",
        *,
        download_results: bool = True,
        cancel_running_job_after: Optional[int] = None,
    ):
        """Create a MultiBackendJobManager."""
        self._stop_thread = None
        self.backends: Dict[str, _Backend] = {}
        self.poll_sleep = poll_sleep
        self._connections: Dict[str, _Backend] = {}

        # An explicit None or "" should also default to "."
        self._root_dir = Path(root_dir or ".")

        self._download_results = download_results

        self._cancel_running_job_after = (
            datetime.timedelta(seconds=cancel_running_job_after) if cancel_running_job_after is not None else None
        )
        self._thread = None
        self._worker_pool = None
        # Generic cache
        self._cache = {}

    def add_backend(
        self,
        name: str,
        connection: Union[Connection, Callable[[], Connection]],
        parallel_jobs: int = 2,
    ):
        """
        Register a backend with a name and a :py:class:`Connection` getter.

        .. note::
           For optimal throughput and responsiveness, it is recommended
           to provide a :py:class:`Connection` instance without its own (blocking) retry behavior,
           since the job manager will do retries in a non-blocking way,
           allowing to take care of other tasks before retrying failed requests.

        :param name:
            Name of the backend.
        :param connection:
            Either a Connection to the backend, or a callable to create a backend connection.
        :param parallel_jobs:
            Maximum number of jobs to allow in parallel on a backend.
        """

        # TODO: Code might become simpler if we turn _Backend into class move this logic there.
        #   We would need to keep add_backend here as part of the public API though.
        #   But the amount of unrelated "stuff to manage" would be less (better cohesion)
        if isinstance(connection, Connection):
            c = connection
            connection = lambda: c
        assert callable(connection)
        # TODO: expose queueing_limit?
        self.backends[name] = _Backend(get_connection=connection, parallel_jobs=parallel_jobs, queueing_limit=10)

    def _get_connection(self, backend_name: str, resilient: bool = True) -> Connection:
        """Get a connection for the backend and optionally make it resilient (adds retry behavior)

        The default is to get a resilient connection, but if necessary you can turn it off with
        resilient=False
        """

        # TODO: Code could be simplified if _Backend is a class and this method is moved there.
        # TODO: Is it better to make this a public method?

        # Reuse the connection if we can, in order to avoid modifying the same connection several times.
        # This is to avoid adding the retry HTTPAdapter multiple times.
        # Remember that the get_connection attribute on _Backend can be a Connection object instead
        # of a callable, so we don't want to assume it is a fresh connection that doesn't have the
        # retry adapter yet.
        if backend_name in self._connections:
            return self._connections[backend_name]

        connection = self.backends[backend_name].get_connection()
        # If we really need it we can skip making it resilient, but by default it should be resilient.
        if resilient:
            self._make_resilient(connection)

        self._connections[backend_name] = connection
        return connection

    @staticmethod
    def _make_resilient(connection):
        """Add an HTTPAdapter that retries the request if it fails.

        Retry for the following HTTP 50x statuses:
        502 Bad Gateway
        503 Service Unavailable
        504 Gateway Timeout
        """
        # TODO: migrate this to now built-in retry configuration of `Connection` or `openeo.util.http.retry_adapter`?
        status_forcelist = [500, 502, 503, 504]
        retries = Retry(
            total=MAX_RETRIES,
            read=MAX_RETRIES,
            other=MAX_RETRIES,
            status=MAX_RETRIES,
            backoff_factor=0.1,
            status_forcelist=status_forcelist,
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )
        connection.session.mount("https://", HTTPAdapter(max_retries=retries))
        connection.session.mount("http://", HTTPAdapter(max_retries=retries))

    @classmethod
    def _normalize_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deprecated, but kept for backwards compatibility
        """
        return cls._column_requirements.normalize_df(df)

    def start_job_thread(self, start_job: Callable[[], BatchJob], job_db: JobDatabaseInterface):
        """
        Start running the jobs in a separate thread, returns afterwards.

        :param start_job:
            A callback which will be invoked with, amongst others,
            the row of the dataframe for which a job should be created and/or started.
            This callable should return a :py:class:`openeo.rest.job.BatchJob` object.

            The following parameters will be passed to ``start_job``:

                ``row`` (:py:class:`pandas.Series`):
                    The row in the pandas dataframe that stores the jobs state and other tracked data.

                ``connection_provider``:
                    A getter to get a connection by backend name.
                    Typically, you would need either the parameter ``connection_provider``,
                    or the parameter ``connection``, but likely you will not need both.

                ``connection`` (:py:class:`Connection`):
                    The :py:class:`Connection` itself, that has already been created.
                    Typically, you would need either the parameter ``connection_provider``,
                    or the parameter ``connection``, but likely you will not need both.

                ``provider`` (``str``):
                    The name of the backend that will run the job.

            You do not have to define all the parameters described below, but if you leave
            any of them out, then remember to include the ``*args`` and ``**kwargs`` parameters.
            Otherwise you will have an exception because :py:meth:`run_jobs` passes unknown parameters to ``start_job``.
        :param job_db:
            Job database to load/store existing job status data and other metadata from/to.
            Can be specified as a path to CSV or Parquet file,
            or as a custom database object following the :py:class:`~openeo.extra.job_management._interface.JobDatabaseInterface` interface.

            .. note::
                Support for Parquet files depends on the ``pyarrow`` package
                as :ref:`optional dependency <installation-optional-dependencies>`.

        .. versionadded:: 0.32.0
        """

        # Resume from existing db
        _log.info(f"Resuming `run_jobs` from existing {job_db}")

        self._stop_thread = False
        self._worker_pool = _JobManagerWorkerThreadPool()

        def run_loop():
            # TODO: support user-provided `stats`
            stats = collections.defaultdict(int)

            while (
                sum(
                    job_db.count_by_status(
                        statuses=["not_started", "created", "queued", "queued_for_start", "running"]
                    ).values()
                )
                > 0
                and not self._stop_thread
            ):
                self._job_update_loop(job_db=job_db, start_job=start_job, stats=stats)
                stats["run_jobs loop"] += 1

                # Show current stats and sleep
                _log.info(f"Job status histogram: {job_db.count_by_status()}. Run stats: {dict(stats)}")
                for _ in range(int(max(1, self.poll_sleep))):
                    time.sleep(1)
                    if self._stop_thread:
                        break

        self._thread = Thread(target=run_loop)
        self._thread.start()

    def stop_job_thread(self, timeout_seconds: Optional[float] = _UNSET):
        """
        Stop the job polling thread.

        :param timeout_seconds: The time to wait for the thread to stop.
            By default, it will wait for 2 times the poll_sleep time.
            Set to None to wait indefinitely.

        .. versionadded:: 0.32.0
        """
        self._worker_pool.shutdown()

        if self._thread is not None:
            self._stop_thread = True
            if timeout_seconds is _UNSET:
                timeout_seconds = 2 * self.poll_sleep
            self._thread.join(timeout_seconds)
            if self._thread.is_alive():
                _log.warning("Job thread did not stop after timeout")
        else:
            _log.error("No job thread to stop")

    def run_jobs(
        self,
        df: Optional[pd.DataFrame] = None,
        start_job: Callable[[], BatchJob] = _start_job_default,
        job_db: Union[str, Path, JobDatabaseInterface, None] = None,
        **kwargs,
    ) -> dict:
        """Runs jobs, specified in a dataframe, and tracks parameters.

        :param df:
            DataFrame that specifies the jobs, and tracks the jobs' statuses. If None, the job_db has to be specified and will be used.

        :param start_job:
            A callback which will be invoked with, amongst others,
            the row of the dataframe for which a job should be created and/or started.
            This callable should return a :py:class:`openeo.rest.job.BatchJob` object.

            The following parameters will be passed to ``start_job``:

                ``row`` (:py:class:`pandas.Series`):
                    The row in the pandas dataframe that stores the jobs state and other tracked data.

                ``connection_provider``:
                    A getter to get a connection by backend name.
                    Typically, you would need either the parameter ``connection_provider``,
                    or the parameter ``connection``, but likely you will not need both.

                ``connection`` (:py:class:`Connection`):
                    The :py:class:`Connection` itself, that has already been created.
                    Typically, you would need either the parameter ``connection_provider``,
                    or the parameter ``connection``, but likely you will not need both.

                ``provider`` (``str``):
                    The name of the backend that will run the job.

            You do not have to define all the parameters described below, but if you leave
            any of them out, then remember to include the ``*args`` and ``**kwargs`` parameters.
            Otherwise you will have an exception because :py:meth:`run_jobs` passes unknown parameters to ``start_job``.

        :param job_db:
            Job database to load/store existing job status data and other metadata from/to.
            Can be specified as a path to CSV or Parquet file,
            or as a custom database object following the :py:class:`~openeo.extra.job_management._interface.JobDatabaseInterface` interface.

            .. note::
                Support for Parquet files depends on the ``pyarrow`` package
                as :ref:`optional dependency <installation-optional-dependencies>`.

        :return: dictionary with stats collected during the job running loop.
            Note that the set of fields in this dictionary is experimental
            and subject to change

        .. versionchanged:: 0.31.0
            Added support for persisting the job metadata in Parquet format.

        .. versionchanged:: 0.31.0
            Replace ``output_file`` argument with ``job_db`` argument,
            which can be a path to a CSV or Parquet file,
            or a user-defined :py:class:`~openeo.extra.job_management._interface.JobDatabaseInterface` object.
            The deprecated ``output_file`` argument is still supported for now.

        .. versionchanged:: 0.33.0
            return a stats dictionary
        """
        # TODO Defining start_jobs as a Protocol might make its usage more clear, and avoid complicated docstrings,

        # Backwards compatibility for deprecated `output_file` argument
        if "output_file" in kwargs:
            if job_db is not None:
                raise ValueError("Only one of `output_file` and `job_db` should be provided")
            warnings.warn(
                "The `output_file` argument is deprecated. Use `job_db` instead.", DeprecationWarning, stacklevel=2
            )
            job_db = kwargs.pop("output_file")
        assert not kwargs, f"Unexpected keyword arguments: {kwargs!r}"

        if isinstance(job_db, (str, Path)):
            job_db = openeo.extra.job_management._job_db.get_job_db(path=job_db)

        if not isinstance(job_db, JobDatabaseInterface):
            raise ValueError(f"Unsupported job_db {job_db!r}")

        if job_db.exists():
            # Resume from existing db
            _log.info(f"Resuming `run_jobs` from existing {job_db}")
        elif df is not None:
            # TODO: start showing deprecation warnings for this usage pattern?
            job_db.initialize_from_df(df)

        # TODO: support user-provided `stats`
        stats = collections.defaultdict(int)

        self._worker_pool = _JobManagerWorkerThreadPool()

        while (
            sum(
                job_db.count_by_status(
                    statuses=["not_started", "created", "queued_for_start", "queued", "running"]
                ).values()
            )
            > 0
        ):
            self._job_update_loop(job_db=job_db, start_job=start_job, stats=stats)
            stats["run_jobs loop"] += 1

            # Show current stats and sleep
            _log.info(f"Job status histogram: {job_db.count_by_status()}. Run stats: {dict(stats)}")
            time.sleep(self.poll_sleep)
            stats["sleep"] += 1

        # TODO; run post process after shutdown once more to ensure completion?
        self._worker_pool.shutdown()

        return stats

    def _job_update_loop(
        self, job_db: JobDatabaseInterface, start_job: Callable[[], BatchJob], stats: Optional[dict] = None
    ):
        """
        Inner loop logic of job management:
        go through the necessary jobs to check for status updates,
        trigger status events, start new jobs when there is room for them, etc.
        """
        if not self.backends:
            raise RuntimeError("No backends registered")

        stats = stats if stats is not None else collections.defaultdict(int)

        with ignore_connection_errors(context="get statuses"):
            jobs_done, jobs_error, jobs_cancel = self._track_statuses(job_db, stats=stats)
            stats["track_statuses"] += 1

        not_started = job_db.get_by_status(statuses=["not_started"], max=200).copy()
        if len(not_started) > 0:
            # Check number of jobs queued/running at each backend
            # TODO: should "created" be included in here? Calling this "running" is quite misleading then.
            #       apparently (see #839/#840) this seemingly simple change makes a lot of MultiBackendJobManager tests flaky
            running = job_db.get_by_status(statuses=["created", "queued", "queued_for_start", "running"])
            queued = running[running["status"] == "queued"]
            running_per_backend = running.groupby("backend_name").size().to_dict()
            queued_per_backend = queued.groupby("backend_name").size().to_dict()
            _log.info(f"{running_per_backend=} {queued_per_backend=}")

            total_added = 0
            for backend_name in self.backends:
                queue_capacity = self.backends[backend_name].queueing_limit - queued_per_backend.get(backend_name, 0)
                run_capacity = self.backends[backend_name].parallel_jobs - running_per_backend.get(backend_name, 0)
                to_add = min(queue_capacity, run_capacity)
                if to_add > 0:
                    for i in not_started.index[total_added : total_added + to_add]:
                        self._launch_job(start_job, df=not_started, i=i, backend_name=backend_name, stats=stats)
                        stats["job launch"] += 1

                        job_db.persist(not_started.loc[i : i + 1])
                        stats["job_db persist"] += 1
                        total_added += 1

        self._process_threadworker_updates(self._worker_pool, job_db=job_db, stats=stats)

        # TODO: move this back closer to the `_track_statuses` call above, once job done/error handling is also handled in threads?
        for job, row in jobs_done:
            self.on_job_done(job, row)

        for job, row in jobs_error:
            self.on_job_error(job, row)

        for job, row in jobs_cancel:
            self.on_job_cancel(job, row)

    def _launch_job(self, start_job, df, i, backend_name, stats: Optional[dict] = None):
        """Helper method for launching jobs

        :param start_job:
            A callback which will be invoked with the row of the dataframe for which a job should be started.
            This callable should return a :py:class:`openeo.rest.job.BatchJob` object.

            See also:
            `MultiBackendJobManager.run_jobs` for the parameters and return type of this callable

            Even though it is called here in `_launch_job` and that is where the constraints
            really come from, the public method `run_jobs` needs to document `start_job` anyway,
            so let's avoid duplication in the docstrings.

        :param df:
            DataFrame that specifies the jobs, and tracks the jobs' statuses.

        :param i:
            index of the job's row in dataframe df

        :param backend_name:
            name of the backend that will execute the job.
        """
        stats = stats if stats is not None else collections.defaultdict(int)

        df.loc[i, "backend_name"] = backend_name
        row = df.loc[i]
        try:
            _log.info(f"Starting job on backend {backend_name} for {row.to_dict()}")
            connection = self._get_connection(backend_name, resilient=True)

            stats["start_job call"] += 1
            job = start_job(
                row=row,
                connection_provider=self._get_connection,
                connection=connection,
                provider=backend_name,
            )
        except requests.exceptions.ConnectionError as e:
            _log.warning(f"Failed to start job for {row.to_dict()}", exc_info=True)
            df.loc[i, "status"] = "start_failed"
            stats["start_job error"] += 1
        else:
            df.loc[i, "start_time"] = rfc3339.now_utc()
            if job:
                df.loc[i, "id"] = job.job_id
                _log.info(f"Job created: {job.job_id}")
                with ignore_connection_errors(context="get status"):
                    status = job.status()
                    stats["job get status"] += 1
                    df.loc[i, "status"] = status
                    if status == "created":
                        # start job if not yet done by callback
                        try:
                            job_con = job.connection
                            # Proactively refresh bearer token (because task in thread will not be able to do that)
                            self._refresh_bearer_token(connection=job_con)
                            task = _JobStartTask(
                                root_url=job_con.root_url,
                                bearer_token=job_con.auth.bearer if isinstance(job_con.auth, BearerAuth) else None,
                                job_id=job.job_id,
                                df_idx=i,
                            )
                            _log.info(f"Submitting task {task} to thread pool")
                            self._worker_pool.submit_task(task)

                            stats["job_queued_for_start"] += 1
                            df.loc[i, "status"] = "queued_for_start"
                        except OpenEoApiError as e:
                            _log.info(f"Failed submitting task {task} to thread pool with error: {e}")
                            df.loc[i, "status"] = "queued_for_start_failed"
                            stats["job queued for start failed"] += 1
            else:
                # TODO: what is this "skipping" about actually?
                df.loc[i, "status"] = "skipped"
                stats["start_job skipped"] += 1

    def _refresh_bearer_token(self, connection: Connection, *, max_age: float = 60) -> None:
        """
        Helper to proactively refresh the bearer (access) token of the connection
        (but not too often, based on `max_age`).
        """
        # TODO: be smarter about timing, e.g. by inspecting expiry of current token?
        now = time.time()
        key = f"connection:{id(connection)}:refresh-time"
        if self._cache.get(key, 0) + max_age < now:
            refreshed = connection.try_access_token_refresh()
            if refreshed:
                self._cache[key] = now
            else:
                _log.warning("Failed to proactively refresh bearer token")

    def _process_threadworker_updates(
        self,
        worker_pool: _JobManagerWorkerThreadPool,
        *,
        job_db: JobDatabaseInterface,
        stats: Dict[str, int],
    ) -> None:
        """
        Fetches completed TaskResult objects from the worker pool and applies
        their db_update and stats_updates. Only existing DataFrame rows
        (matched by df_idx) are upserted via job_db.persist(). Any results
        targeting unknown df_idx indices are logged as errors but not persisted.

        :param worker_pool: Thread-pool managing asynchronous Task executes
        :param job_db:      Interface to append/upsert to the job database
        :param stats:       Dictionary accumulating statistic counters
        """
        # Retrieve completed task results immediately
        results, _ = worker_pool.process_futures(timeout=0)

        # Collect update dicts
        updates: List[Dict[str, Any]] = []
        for res in results:
            # Process database updates
            if res.db_update:
                try:
                    updates.append(
                        {
                            "id": res.job_id,
                            "df_idx": res.df_idx,
                            **res.db_update,
                        }
                    )
                except Exception as e:
                    _log.error(f"Skipping invalid db_update {res.db_update!r} for job {res.job_id!r}: {e}")

            # Process stats updates
            if res.stats_update:
                try:
                    for key, val in res.stats_update.items():
                        count = int(val)
                        stats[key] = stats.get(key, 0) + count
                except Exception as e:
                    _log.error(f"Skipping invalid stats_update {res.stats_update!r} for job {res.job_id!r}: {e}")

        # No valid updates: nothing to persist
        if not updates:
            return

        # Build update DataFrame and persist
        df_updates = job_db.get_by_indices(indices=set(u["df_idx"] for u in updates))
        df_updates.update(pd.DataFrame(updates).set_index("df_idx", drop=True), overwrite=True)
        job_db.persist(df_updates)
        stats["job_db persist"] = stats.get("job_db persist", 0) + 1

    def on_job_done(self, job: BatchJob, row):
        """
        Handles jobs that have finished. Can be overridden to provide custom behaviour.

        Default implementation downloads the results into a folder containing the title.

        :param job: The job that has finished.
        :param row: DataFrame row containing the job's metadata.
        """
        # TODO: param `row` is never accessed in this method. Remove it? Is this intended for future use?
        if self._download_results:
            job_metadata = job.describe()
            job_dir = self.get_job_dir(job.job_id)
            metadata_path = self.get_job_metadata_path(job.job_id)

            self.ensure_job_dir_exists(job.job_id)
            job.get_results().download_files(target=job_dir)

            with metadata_path.open("w", encoding="utf-8") as f:
                json.dump(job_metadata, f, ensure_ascii=False)

    def on_job_error(self, job: BatchJob, row):
        """
        Handles jobs that stopped with errors. Can be overridden to provide custom behaviour.

        Default implementation writes the error logs to a JSON file.

        :param job: The job that has finished.
        :param row: DataFrame row containing the job's metadata.
        """
        # TODO: param `row` is never accessed in this method. Remove it? Is this intended for future use?

        error_logs = job.logs(level="error")
        error_log_path = self.get_error_log_path(job.job_id)

        if len(error_logs) > 0:
            self.ensure_job_dir_exists(job.job_id)
            error_log_path.write_text(json.dumps(error_logs, indent=2))

    def on_job_cancel(self, job: BatchJob, row):
        """
        Handle a job that was cancelled. Can be overridden to provide custom behaviour.

        Default implementation does not do anything.

        :param job: The job that was canceled.
        :param row: DataFrame row containing the job's metadata.
        """
        pass

    def _cancel_prolonged_job(self, job: BatchJob, row):
        """Cancel the job if it has been running for too long."""
        try:
            # Ensure running start time is valid
            job_running_start_time = rfc3339.parse_datetime(row.get("running_start_time"), with_timezone=True)

            # Parse the current time into a datetime object with timezone info
            current_time = rfc3339.parse_datetime(rfc3339.now_utc(), with_timezone=True)

            # Calculate the elapsed time between job start and now
            elapsed = current_time - job_running_start_time

            if elapsed > self._cancel_running_job_after:
                _log.info(
                    f"Cancelling long-running job {job.job_id} (after {elapsed}, running since {job_running_start_time})"
                )
                job.stop()

        except Exception as e:
            _log.error(f"Unexpected error while handling job {job.job_id}: {e}")

    def get_job_dir(self, job_id: str) -> Path:
        """Path to directory where job metadata, results and error logs are be saved."""
        return self._root_dir / f"job_{job_id}"

    def get_error_log_path(self, job_id: str) -> Path:
        """Path where error log file for the job is saved."""
        return self.get_job_dir(job_id) / f"job_{job_id}_errors.json"

    def get_job_metadata_path(self, job_id: str) -> Path:
        """Path where job metadata file is saved."""
        return self.get_job_dir(job_id) / f"job_{job_id}.json"

    def ensure_job_dir_exists(self, job_id: str) -> Path:
        """Create the job folder if it does not exist yet."""
        job_dir = self.get_job_dir(job_id)
        if not job_dir.exists():
            job_dir.mkdir(parents=True)

    def _track_statuses(self, job_db: JobDatabaseInterface, stats: Optional[dict] = None) -> Tuple[List, List, List]:
        """
        Tracks status (and stats) of running jobs (in place).
        Optionally cancels jobs when running too long.
        """
        stats = stats if stats is not None else collections.defaultdict(int)

        active = job_db.get_by_status(statuses=["created", "queued", "queued_for_start", "running"]).copy()

        jobs_done = []
        jobs_error = []
        jobs_cancel = []

        for i in active.index:
            job_id = active.loc[i, "id"]
            backend_name = active.loc[i, "backend_name"]
            previous_status = active.loc[i, "status"]

            try:
                con = self._get_connection(backend_name)
                the_job = con.job(job_id)
                job_metadata = the_job.describe()
                stats["job describe"] += 1
                new_status = job_metadata["status"]

                _log.info(
                    f"Status of job {job_id!r} (on backend {backend_name}) is {new_status!r} (previously {previous_status!r})"
                )

                if previous_status != "finished" and new_status == "finished":
                    stats["job finished"] += 1
                    jobs_done.append((the_job, active.loc[i]))

                if previous_status != "error" and new_status == "error":
                    stats["job failed"] += 1
                    jobs_error.append((the_job, active.loc[i]))

                if new_status == "canceled":
                    stats["job canceled"] += 1
                    jobs_cancel.append((the_job, active.loc[i]))

                if previous_status in {"created", "queued", "queued_for_start"} and new_status == "running":
                    stats["job started running"] += 1
                    active.loc[i, "running_start_time"] = rfc3339.now_utc()

                if self._cancel_running_job_after and new_status == "running":
                    if not active.loc[i, "running_start_time"] or pd.isna(active.loc[i, "running_start_time"]):
                        _log.warning(
                            f"Unknown 'running_start_time' for running job {job_id}. Using current time as an approximation."
                        )
                        stats["job started running"] += 1
                        active.loc[i, "running_start_time"] = rfc3339.now_utc()

                    self._cancel_prolonged_job(the_job, active.loc[i])

                active.loc[i, "status"] = new_status

                # TODO: there is well hidden coupling here with "cpu", "memory" and "duration" from `_normalize_df`
                for key in job_metadata.get("usage", {}).keys():
                    if key in active.columns:
                        active.loc[i, key] = _format_usage_stat(job_metadata, key)
                if "costs" in job_metadata.keys():
                    active.loc[i, "costs"] = job_metadata.get("costs")

            except OpenEoApiError as e:
                # TODO: inspect status code and e.g. differentiate between 4xx/5xx
                stats["job tracking error"] += 1
                _log.warning(f"Error while tracking status of job {job_id!r} on backend {backend_name}: {e!r}")

        stats["job_db persist"] += 1
        job_db.persist(active)

        return jobs_done, jobs_error, jobs_cancel


def _format_usage_stat(job_metadata: dict, field: str) -> str:
    value = deep_get(job_metadata, "usage", field, "value", default=0)
    unit = deep_get(job_metadata, "usage", field, "unit", default="")
    return f"{value} {unit}".strip()


@contextlib.contextmanager
def ignore_connection_errors(context: Optional[str] = None, sleep: int = 5):
    """Context manager to ignore connection errors."""
    # TODO: move this out of this module and make it a more public utility?
    try:
        yield
    except requests.exceptions.ConnectionError as e:
        _log.warning(f"Ignoring connection error (context {context or 'n/a'}): {e}")
        # Back off a bit
        time.sleep(sleep)

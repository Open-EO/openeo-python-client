import abc
import collections
import contextlib
import dataclasses
import datetime
import json
import logging
import re
import time
import warnings
from pathlib import Path
from threading import Thread
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Union,
)

import numpy
import pandas as pd
import requests
import shapely.errors
import shapely.geometry.base
import shapely.wkt
from requests.adapters import HTTPAdapter, Retry

from openeo import BatchJob, Connection
from openeo.internal.processes.parse import (
    Parameter,
    Process,
    parse_remote_process_definition,
)
from openeo.rest import OpenEoApiError
from openeo.util import LazyLoadCache, deep_get, repr_truncate, rfc3339

_log = logging.getLogger(__name__)


class _Backend(NamedTuple):
    """Container for backend info/settings"""

    # callable to create a backend connection
    get_connection: Callable[[], Connection]
    # Maximum number of jobs to allow in parallel on a backend
    parallel_jobs: int


MAX_RETRIES = 5

# Sentinel value to indicate that a parameter was not set
_UNSET = object()


class JobDatabaseInterface(metaclass=abc.ABCMeta):
    """
    Interface for a database of job metadata to use with the :py:class:`MultiBackendJobManager`,
    allowing to regularly persist the job metadata while polling the job statuses
    and resume/restart the job tracking after it was interrupted.

    .. versionadded:: 0.31.0
    """

    @abc.abstractmethod
    def exists(self) -> bool:
        """Does the job database already exist, to read job data from?"""
        ...

    @abc.abstractmethod
    def persist(self, df: pd.DataFrame):
        """
        Store job data to the database.
        The provided dataframe may contain partial information, which is merged into the larger database.

        :param df: job data to store.
        """
        ...

    @abc.abstractmethod
    def count_by_status(self, statuses: Iterable[str] = ()) -> dict:
        """
        Retrieve the number of jobs per status.

        :param statuses: List/set of statuses to include. If empty, all statuses are included.

        :return: dictionary with status as key and the count as value.
        """
        ...

    @abc.abstractmethod
    def get_by_status(self, statuses: List[str], max=None) -> pd.DataFrame:
        """
        Returns a dataframe with jobs, filtered by status.

        :param statuses: List of statuses to include.
        :param max: Maximum number of jobs to return.

        :return: DataFrame with jobs filtered by status.
        """
        ...

def _start_job_default(row: pd.Series, connection: Connection, *args, **kwargs):
    raise NotImplementedError("No 'start_job' callable provided")


@dataclasses.dataclass(frozen=True)
class _ColumnProperties:
    """Expected/required properties of a column in the job manager related dataframes"""

    dtype: str = "object"
    default: Any = None


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

    :param cancel_running_job_after:
        Optional temporal limit (in seconds) after which running jobs should be canceled
        by the job manager.

    .. versionadded:: 0.14.0

    .. versionchanged:: 0.32.0
        Added ``cancel_running_job_after`` parameter.
    """

    # Expected columns in the job DB dataframes.
    # TODO: make this part of public API when settled?
    _COLUMN_REQUIREMENTS: Mapping[str, _ColumnProperties] = {
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

    def __init__(
        self,
        poll_sleep: int = 60,
        root_dir: Optional[Union[str, Path]] = ".",
        *,
        cancel_running_job_after: Optional[int] = None,
    ):
        """Create a MultiBackendJobManager."""
        self._stop_thread = None
        self.backends: Dict[str, _Backend] = {}
        self.poll_sleep = poll_sleep
        self._connections: Dict[str, _Backend] = {}

        # An explicit None or "" should also default to "."
        self._root_dir = Path(root_dir or ".")

        self._cancel_running_job_after = (
            datetime.timedelta(seconds=cancel_running_job_after) if cancel_running_job_after is not None else None
        )
        self._thread = None

    def add_backend(
        self,
        name: str,
        connection: Union[Connection, Callable[[], Connection]],
        parallel_jobs: int = 2,
    ):
        """
        Register a backend with a name and a Connection getter.

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
        self.backends[name] = _Backend(get_connection=connection, parallel_jobs=parallel_jobs)

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
        # TODO: refactor this helper out of this class and unify with `openeo_driver.util.http.requests_with_retry`
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
        Normalize given pandas dataframe (creating a new one):
        ensure we have the required columns.

        :param df: The dataframe to normalize.
        :return: a new dataframe that is normalized.
        """
        new_columns = {col: req.default for (col, req) in cls._COLUMN_REQUIREMENTS.items() if col not in df.columns}
        df = df.assign(**new_columns)

        return df

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
            or as a custom database object following the :py:class:`JobDatabaseInterface` interface.

            .. note::
                Support for Parquet files depends on the ``pyarrow`` package
                as :ref:`optional dependency <installation-optional-dependencies>`.

        .. versionadded:: 0.32.0
        """

        # Resume from existing db
        _log.info(f"Resuming `run_jobs` from existing {job_db}")

        self._stop_thread = False

        def run_loop():

            # TODO: support user-provided `stats`
            stats = collections.defaultdict(int)

            while (
                sum(job_db.count_by_status(statuses=["not_started", "created", "queued", "running"]).values()) > 0
                and not self._stop_thread
            ):
                self._job_update_loop(job_db=job_db, start_job=start_job)
                stats["run_jobs loop"] += 1

                _log.info(f"Job status histogram: {job_db.count_by_status()}. Run stats: {dict(stats)}")
                # Do sequence of micro-sleeps to allow for quick thread exit
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
            or as a custom database object following the :py:class:`JobDatabaseInterface` interface.

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
            or a user-defined :py:class:`JobDatabaseInterface` object.
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
            job_db = get_job_db(path=job_db)

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

        while sum(job_db.count_by_status(statuses=["not_started", "created", "queued", "running"]).values()) > 0:
            self._job_update_loop(job_db=job_db, start_job=start_job, stats=stats)
            stats["run_jobs loop"] += 1

            # Show current stats and sleep
            _log.info(f"Job status histogram: {job_db.count_by_status()}. Run stats: {dict(stats)}")
            time.sleep(self.poll_sleep)
            stats["sleep"] += 1

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
            self._track_statuses(job_db, stats=stats)
            stats["track_statuses"] += 1

        not_started = job_db.get_by_status(statuses=["not_started"], max=200).copy()
        if len(not_started) > 0:
            # Check number of jobs running at each backend
            running = job_db.get_by_status(statuses=["created", "queued", "running"])
            stats["job_db get_by_status"] += 1
            per_backend = running.groupby("backend_name").size().to_dict()
            _log.info(f"Running per backend: {per_backend}")
            total_added = 0
            for backend_name in self.backends:
                backend_load = per_backend.get(backend_name, 0)
                if backend_load < self.backends[backend_name].parallel_jobs:
                    to_add = self.backends[backend_name].parallel_jobs - backend_load
                    for i in not_started.index[total_added : total_added + to_add]:
                        self._launch_job(start_job, df=not_started, i=i, backend_name=backend_name, stats=stats)
                        stats["job launch"] += 1

                        job_db.persist(not_started.loc[i : i + 1])
                        stats["job_db persist"] += 1
                        total_added += 1

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
            df.loc[i, "start_time"] = rfc3339.utcnow()
            if job:
                df.loc[i, "id"] = job.job_id
                with ignore_connection_errors(context="get status"):
                    status = job.status()
                    stats["job get status"] += 1
                    df.loc[i, "status"] = status
                    if status == "created":
                        # start job if not yet done by callback
                        try:
                            job.start()
                            stats["job start"] += 1
                            df.loc[i, "status"] = job.status()
                            stats["job get status"] += 1
                        except OpenEoApiError as e:
                            _log.error(e)
                            df.loc[i, "status"] = "start_failed"
                            stats["job start error"] += 1
            else:
                # TODO: what is this "skipping" about actually?
                df.loc[i, "status"] = "skipped"
                stats["start_job skipped"] += 1

    def on_job_done(self, job: BatchJob, row):
        """
        Handles jobs that have finished. Can be overridden to provide custom behaviour.

        Default implementation downloads the results into a folder containing the title.

        :param job: The job that has finished.
        :param row: DataFrame row containing the job's metadata.
        """
        # TODO: param `row` is never accessed in this method. Remove it? Is this intended for future use?

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
            current_time = rfc3339.parse_datetime(rfc3339.utcnow(), with_timezone=True)

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

    def _track_statuses(self, job_db: JobDatabaseInterface, stats: Optional[dict] = None):
        """
        Tracks status (and stats) of running jobs (in place).
        Optionally cancels jobs when running too long.
        """
        stats = stats if stats is not None else collections.defaultdict(int)

        active = job_db.get_by_status(statuses=["created", "queued", "running"]).copy()
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

                if new_status == "finished":
                    stats["job finished"] += 1
                    self.on_job_done(the_job, active.loc[i])

                if previous_status != "error" and new_status == "error":
                    stats["job failed"] += 1
                    self.on_job_error(the_job, active.loc[i])

                if previous_status in {"created", "queued"} and new_status == "running":
                    stats["job started running"] += 1
                    active.loc[i, "running_start_time"] = rfc3339.utcnow()

                if new_status == "canceled":
                    stats["job canceled"] += 1
                    self.on_job_cancel(the_job, active.loc[i])

                if self._cancel_running_job_after and new_status == "running":
                    if  (not active.loc[i, "running_start_time"] or pd.isna(active.loc[i, "running_start_time"])):
                        _log.warning(
                            f"Unknown 'running_start_time' for running job {job_id}. Using current time as an approximation."
                            )
                        stats["job started running"] += 1
                        active.loc[i, "running_start_time"] = rfc3339.utcnow()

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


class FullDataFrameJobDatabase(JobDatabaseInterface):

    def __init__(self):
        super().__init__()
        self._df = None

    def initialize_from_df(self, df: pd.DataFrame, *, on_exists: str = "error"):
        """
        Initialize the job database from a given dataframe,
        which will be first normalized to be compatible
        with :py:class:`MultiBackendJobManager` usage.

        :param df: dataframe with some columns your ``start_job`` callable expects
        :param on_exists: what to do when the job database already exists (persisted on disk):
            - "error": (default) raise an exception
            - "skip": work with existing database, ignore given dataframe and skip any initialization

        :return: initialized job database.

        .. versionadded:: 0.33.0
        """
        # TODO: option to provide custom MultiBackendJobManager subclass with custom normalize?
        if self.exists():
            if on_exists == "skip":
                return self
            elif on_exists == "error":
                raise FileExistsError(f"Job database {self!r} already exists.")
            else:
                # TODO handle other on_exists modes: e.g. overwrite, merge, ...
                raise ValueError(f"Invalid on_exists={on_exists!r}")
        df = MultiBackendJobManager._normalize_df(df)
        self.persist(df)
        # Return self to allow chaining with constructor.
        return self

    @abc.abstractmethod
    def read(self) -> pd.DataFrame:
        """
        Read job data from the database as pandas DataFrame.

        :return: loaded job data.
        """
        ...

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = self.read()
        return self._df

    def count_by_status(self, statuses: Iterable[str] = ()) -> dict:
        status_histogram = self.df.groupby("status").size().to_dict()
        statuses = set(statuses)
        if statuses:
            status_histogram = {k: v for k, v in status_histogram.items() if k in statuses}
        return status_histogram

    def get_by_status(self, statuses, max=None) -> pd.DataFrame:
        """
        Returns a dataframe with jobs, filtered by status.

        :param statuses: List of statuses to include.
        :param max: Maximum number of jobs to return.

        :return: DataFrame with jobs filtered by status.
        """
        df = self.df
        filtered = df[df.status.isin(statuses)]
        return filtered.head(max) if max is not None else filtered

    def _merge_into_df(self, df: pd.DataFrame):
        if self._df is not None:
            self._df.update(df, overwrite=True)
        else:
            self._df = df


class CsvJobDatabase(FullDataFrameJobDatabase):
    """
    Persist/load job metadata with a CSV file.

    :implements: :py:class:`JobDatabaseInterface`
    :param path: Path to local CSV file.

    .. note::
        Support for GeoPandas dataframes depends on the ``geopandas`` package
        as :ref:`optional dependency <installation-optional-dependencies>`.

    .. versionadded:: 0.31.0
    """

    def __init__(self, path: Union[str, Path]):
        super().__init__()
        self.path = Path(path)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)!r})"

    def exists(self) -> bool:
        return self.path.exists()

    def _is_valid_wkt(self, wkt: str) -> bool:
        try:
            shapely.wkt.loads(wkt)
            return True
        except shapely.errors.WKTReadingError:
            return False

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(
            self.path,
            # TODO: possible to avoid hidden coupling with MultiBackendJobManager here?
            dtype={c: r.dtype for (c, r) in MultiBackendJobManager._COLUMN_REQUIREMENTS.items()},
        )
        if (
            "geometry" in df.columns
            and df["geometry"].dtype.name != "geometry"
            and self._is_valid_wkt(df["geometry"].iloc[0])
        ):
            import geopandas

            # `df.to_csv()` in `persist()` has encoded geometries as WKT, so we decode that here.
            df = geopandas.GeoDataFrame(df, geometry=geopandas.GeoSeries.from_wkt(df["geometry"]))
        return df

    def persist(self, df: pd.DataFrame):
        self._merge_into_df(df)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(self.path, index=False)


class ParquetJobDatabase(FullDataFrameJobDatabase):
    """
    Persist/load job metadata with a Parquet file.

    :implements: :py:class:`JobDatabaseInterface`
    :param path: Path to the Parquet file.

    .. note::
        Support for Parquet files depends on the ``pyarrow`` package
        as :ref:`optional dependency <installation-optional-dependencies>`.

        Support for GeoPandas dataframes depends on the ``geopandas`` package
        as :ref:`optional dependency <installation-optional-dependencies>`.

    .. versionadded:: 0.31.0
    """

    def __init__(self, path: Union[str, Path]):
        super().__init__()
        self.path = Path(path)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)!r})"

    def exists(self) -> bool:
        return self.path.exists()

    def read(self) -> pd.DataFrame:
        # Unfortunately, a naive `pandas.read_parquet()` does not easily allow
        # reconstructing geometries from a GeoPandas Parquet file.
        # And vice-versa, `geopandas.read_parquet()` does not support reading
        # Parquet file without geometries.
        # So we have to guess which case we have.
        # TODO is there a cleaner way to do this?
        import pyarrow.parquet

        metadata = pyarrow.parquet.read_metadata(self.path)
        if b"geo" in metadata.metadata:
            import geopandas

            return geopandas.read_parquet(self.path)
        else:
            return pd.read_parquet(self.path)

    def persist(self, df: pd.DataFrame):
        self._merge_into_df(df)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_parquet(self.path, index=False)


def get_job_db(path: Union[str, Path]) -> JobDatabaseInterface:
    """
    Factory to get a job database at a given path,
    guessing the database type from filename extension.

    :param path: path to job database file.

    .. versionadded:: 0.33.0
    """
    path = Path(path)
    if path.suffix.lower() in {".csv"}:
        job_db = CsvJobDatabase(path=path)
    elif path.suffix.lower() in {".parquet", ".geoparquet"}:
        job_db = ParquetJobDatabase(path=path)
    else:
        raise ValueError(f"Could not guess job database type from {path!r}")
    return job_db


def create_job_db(path: Union[str, Path], df: pd.DataFrame, *, on_exists: str = "error"):
    """
    Factory to create a job database at given path,
    initialized from a given dataframe,
    and its database type guessed from filename extension.

    :param path: Path to the job database file.
    :param df: DataFrame to store in the job database.
    :param on_exists: What to do when the job database already exists:
        - "error": (default) raise an exception
        - "skip": work with existing database, ignore given dataframe and skip any initialization

    .. versionadded:: 0.33.0
    """
    job_db = get_job_db(path)
    if isinstance(job_db, FullDataFrameJobDatabase):
        job_db.initialize_from_df(df=df, on_exists=on_exists)
    else:
        raise NotImplementedError(f"Initialization of {type(job_db)} is not supported.")
    return job_db


class ProcessBasedJobCreator:
    """
    Batch job creator
    (to be used together with :py:class:`MultiBackendJobManager`)
    that takes a parameterized openEO process definition
    (e.g a user-defined process (UDP) or a remote openEO process definition),
    and creates a batch job
    for each row of the dataframe managed by the :py:class:`MultiBackendJobManager`
    by filling in the process parameters with corresponding row values.

    .. seealso::
        See :ref:`job-management-with-process-based-job-creator`
        for more information and examples.

    Process parameters are linked to dataframe columns by name.
    While this intuitive name-based matching should cover most use cases,
    there are additional options for overrides or fallbacks:

    -   When provided, ``parameter_column_map`` will be consulted
        for resolving a process parameter name (key in the dictionary)
        to a desired dataframe column name (corresponding value).
    -   One common case is handled automatically as convenience functionality.

        When:

        - ``parameter_column_map`` is not provided (or set to ``None``),
        - and there is a *single parameter* that accepts inline GeoJSON geometries,
        - and the dataframe is a GeoPandas dataframe with a *single geometry* column,

        then this parameter and this geometries column will be linked automatically.

    -   If a parameter can not be matched with a column by name as described above,
        a default value will be picked,
        first by looking in ``parameter_defaults`` (if provided),
        and then by looking up the default value from the parameter schema in the process definition.
    -   Finally if no (default) value can be determined and the parameter
        is not flagged as optional, an error will be raised.


    :param process_id: (optional) openEO process identifier.
        Can be omitted when working with a remote process definition
        that is fully defined with a URL in the ``namespace`` parameter.
    :param namespace: (optional) openEO process namespace.
        Typically used to provide a URL to a remote process definition.
    :param parameter_defaults: (optional) default values for process parameters,
        to be used when not available in the dataframe managed by
        :py:class:`MultiBackendJobManager`.
    :param parameter_column_map: Optional overrides
        for linking process parameters to dataframe columns:
        mapping of process parameter names as key
        to dataframe column names as value.

    .. versionadded:: 0.33.0

    .. warning::
        This is an experimental API subject to change,
        and we greatly welcome
        `feedback and suggestions for improvement <https://github.com/Open-EO/openeo-python-client/issues>`_.

    """

    def __init__(
        self,
        *,
        process_id: Optional[str] = None,
        namespace: Union[str, None] = None,
        parameter_defaults: Optional[dict] = None,
        parameter_column_map: Optional[dict] = None,
    ):
        if process_id is None and namespace is None:
            raise ValueError("At least one of `process_id` and `namespace` should be provided.")
        self._process_id = process_id
        self._namespace = namespace
        self._parameter_defaults = parameter_defaults or {}
        self._parameter_column_map = parameter_column_map
        self._cache = LazyLoadCache()

    def _get_process_definition(self, connection: Connection) -> Process:
        if isinstance(self._namespace, str) and re.match("https?://", self._namespace):
            # Remote process definition handling
            return self._cache.get(
                key=("remote_process_definition", self._namespace, self._process_id),
                load=lambda: parse_remote_process_definition(namespace=self._namespace, process_id=self._process_id),
            )
        elif self._namespace is None:
            # Handling of a user-specific UDP
            udp_raw = connection.user_defined_process(self._process_id).describe()
            return Process.from_dict(udp_raw)
        else:
            raise NotImplementedError(
                f"Unsupported process definition source udp_id={self._process_id!r} namespace={self._namespace!r}"
            )

    def start_job(self, row: pd.Series, connection: Connection, **_) -> BatchJob:
        """
        Implementation of the ``start_job`` callable interface
        of :py:meth:`MultiBackendJobManager.run_jobs`
        to create a job based on given dataframe row

        :param row: The row in the pandas dataframe that stores the jobs state and other tracked data.
        :param connection: The connection to the backend.
        """
        # TODO: refactor out some methods, for better reuse and decoupling:
        #       `get_arguments()` (to build the arguments dictionary), `get_cube()` (to create the cube),

        process_definition = self._get_process_definition(connection=connection)
        process_id = process_definition.id
        parameters = process_definition.parameters or []

        if self._parameter_column_map is None:
            self._parameter_column_map = self._guess_parameter_column_map(parameters=parameters, row=row)

        arguments = {}
        for parameter in parameters:
            param_name = parameter.name
            column_name = self._parameter_column_map.get(param_name, param_name)
            if column_name in row.index:
                # Get value from dataframe row
                value = row.loc[column_name]
            elif param_name in self._parameter_defaults:
                # Fallback on default values from constructor
                value = self._parameter_defaults[param_name]
            elif parameter.has_default():
                # Explicitly use default value from parameter schema
                value = parameter.default
            elif parameter.optional:
                # Skip optional parameters without any fallback default value
                continue
            else:
                raise ValueError(f"Missing required parameter {param_name !r} for process {process_id!r}")

            # Prepare some values/dtypes for JSON encoding
            if isinstance(value, numpy.integer):
                value = int(value)
            elif isinstance(value, numpy.number):
                value = float(value)
            elif isinstance(value, shapely.geometry.base.BaseGeometry):
                value = shapely.geometry.mapping(value)

            arguments[param_name] = value

        cube = connection.datacube_from_process(process_id=process_id, namespace=self._namespace, **arguments)

        title = row.get("title", f"Process {process_id!r} with {repr_truncate(arguments)}")
        description = row.get("description", f"Process {process_id!r} (namespace {self._namespace}) with {arguments}")
        job = connection.create_job(cube, title=title, description=description)

        return job

    def __call__(self, *arg, **kwargs) -> BatchJob:
        """Syntactic sugar for calling :py:meth:`start_job`."""
        return self.start_job(*arg, **kwargs)

    @staticmethod
    def _guess_parameter_column_map(parameters: List[Parameter], row: pd.Series) -> dict:
        """
        Guess parameter-column mapping from given parameter list and dataframe row
        """
        parameter_column_map = {}
        # Geometry based mapping: try to automatically map geometry columns to geojson parameters
        geojson_parameters = [p.name for p in parameters if p.schema.accepts_geojson()]
        geometry_columns = [i for (i, v) in row.items() if isinstance(v, shapely.geometry.base.BaseGeometry)]
        if geojson_parameters and geometry_columns:
            if len(geojson_parameters) == 1 and len(geometry_columns) == 1:
                # Most common case: one geometry parameter and one geometry column: can be mapped naively
                parameter_column_map[geojson_parameters[0]] = geometry_columns[0]
            elif all(p in geometry_columns for p in geojson_parameters):
                # Each geometry param has geometry column with same name: easy to map
                parameter_column_map.update((p, p) for p in geojson_parameters)
            else:
                raise RuntimeError(
                    f"Problem with mapping geometry columns ({geometry_columns}) to process parameters ({geojson_parameters})"
                )
        _log.debug(f"Guessed parameter-column map: {parameter_column_map}")
        return parameter_column_map

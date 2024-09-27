import abc
import contextlib
import datetime
import json
import logging
import time
import warnings
from pathlib import Path
from threading import Thread
from typing import Callable, Dict, List, NamedTuple, Optional, Union

import pandas as pd
import requests
import shapely.errors
import shapely.wkt
from requests.adapters import HTTPAdapter, Retry

from openeo import BatchJob, Connection
from openeo.rest import OpenEoApiError
from openeo.util import deep_get, rfc3339

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
    def read(self) -> pd.DataFrame:
        """
        Read job data from the database as pandas DataFrame.

        :return: loaded job data.
        """
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
    def count_by_status(self, statuses: List[str]) -> dict:
        """
        Retrieve the number of jobs per status.

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

    .. versionadded:: 0.14.0
    """

    def __init__(
        self,
        poll_sleep: int = 60,
        root_dir: Optional[Union[str, Path]] = ".",
        *,
        cancel_running_job_after: Optional[int] = None,
    ):
        """Create a MultiBackendJobManager.

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

        :param cancel_running_job_after [seconds]:
            Optional temporal limit (in seconds) after which running jobs should be canceled
            by the job manager.

        .. versionchanged:: 0.32.0
            Added `cancel_running_job_after` parameter.
        """
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

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure we have the required columns and the expected type for the geometry column.

        :param df: The dataframe to normalize.
        :return: a new dataframe that is normalized.
        """
        # TODO: this was originally an internal helper, but we need a clean public API for the user

        # check for some required columns.
        required_with_default = [
            ("status", "not_started"),
            ("id", None),
            ("start_time", None),
            ("running_start_time", None),
            # TODO: columns "cpu", "memory", "duration" are not referenced directly
            #       within MultiBackendJobManager making it confusing to claim they are required.
            #       However, they are through assumptions about job "usage" metadata in `_track_statuses`.
            #       => proposed solution: allow to configure usage columns when adding a backend
            ("cpu", None),
            ("memory", None),
            ("duration", None),
            ("backend_name", None),
        ]
        new_columns = {col: val for (col, val) in required_with_default if col not in df.columns}
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
        df = job_db.read()

        self._stop_thread = False
        def run_loop():
            while (
                sum(job_db.count_by_status(statuses=["not_started", "created", "queued", "running"]).values()) > 0
                and not self._stop_thread
            ):
                self._job_update_loop(df, job_db, start_job)

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
        df: Optional[pd.DataFrame],
        start_job: Callable[[], BatchJob],
        job_db: Union[str, Path, JobDatabaseInterface, None] = None,
        **kwargs,
    ):
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

        .. versionchanged:: 0.31.0
            Added support for persisting the job metadata in Parquet format.

        .. versionchanged:: 0.31.0
            Replace ``output_file`` argument with ``job_db`` argument,
            which can be a path to a CSV or Parquet file,
            or a user-defined :py:class:`JobDatabaseInterface` object.
            The deprecated ``output_file`` argument is still supported for now.
        """
        # TODO: Defining start_jobs as a Protocol might make its usage more clear, and avoid complicated doctrings,
        #   but Protocols are only supported in Python 3.8 and higher.

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
            job_db_path = Path(job_db)
            if job_db_path.suffix.lower() == ".csv":
                job_db = CsvJobDatabase(path=job_db_path)
            elif job_db_path.suffix.lower() == ".parquet":
                job_db = ParquetJobDatabase(path=job_db_path)
            else:
                raise ValueError(f"Unsupported job database file type {job_db_path!r}")

        if not isinstance(job_db, JobDatabaseInterface):
            raise ValueError(f"Unsupported job_db {job_db!r}")

        if job_db.exists():
            # Resume from existing db
            _log.info(f"Resuming `run_jobs` from existing {job_db}")
        elif df is not None:
            df = self._normalize_df(df)
            job_db.persist(df)

        while sum(job_db.count_by_status(statuses=["not_started", "created", "queued", "running"]).values()) > 0:
            self._job_update_loop(df, job_db, start_job)
            time.sleep(self.poll_sleep)

    def _job_update_loop(self, df, job_db, start_job):
        """
        Inner loop logic of job management:
        go through the necessary jobs to check for status updates,
        trigger status events, start new jobs when there is room for them, etc.
        """
        with ignore_connection_errors(context="get statuses"):
            self._track_statuses(job_db)

        not_started = job_db.get_by_status(statuses=["not_started"], max=200)
        if len(not_started) > 0:
            # Check number of jobs running at each backend
            running = job_db.get_by_status(statuses=["created", "queued", "running"])
            per_backend = running.groupby("backend_name").size().to_dict()
            _log.info(f"Running per backend: {per_backend}")
            for backend_name in self.backends:
                backend_load = per_backend.get(backend_name, 0)
                if backend_load < self.backends[backend_name].parallel_jobs:
                    to_add = self.backends[backend_name].parallel_jobs - backend_load
                    to_launch = not_started.iloc[0:to_add]
                    for i in to_launch.index:
                        self._launch_job(start_job, not_started, i, backend_name)
                        job_db.persist(to_launch)

    def _launch_job(self, start_job, df, i, backend_name):
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

        df.loc[i, "backend_name"] = backend_name
        row = df.loc[i]
        try:
            _log.info(f"Starting job on backend {backend_name} for {row.to_dict()}")
            connection = self._get_connection(backend_name, resilient=True)

            job = start_job(
                row=row,
                connection_provider=self._get_connection,
                connection=connection,
                provider=backend_name,
            )
        except requests.exceptions.ConnectionError as e:
            _log.warning(f"Failed to start job for {row.to_dict()}", exc_info=True)
            df.loc[i, "status"] = "start_failed"
        else:
            df.loc[i, "start_time"] = rfc3339.utcnow()
            if job:
                df.loc[i, "id"] = job.job_id
                with ignore_connection_errors(context="get status"):
                    status = job.status()
                    df.loc[i, "status"] = status
                    if status == "created":
                        # start job if not yet done by callback
                        try:
                            job.start()
                            df.loc[i, "status"] = job.status()
                        except OpenEoApiError as e:
                            _log.error(e)
                            df.loc[i, "status"] = "start_failed"
            else:
                df.loc[i, "status"] = "skipped"

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

        with open(metadata_path, "w") as f:
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
        job_running_start_time = rfc3339.parse_datetime(row["running_start_time"], with_timezone=True)
        elapsed = datetime.datetime.now(tz=datetime.timezone.utc) - job_running_start_time
        if elapsed > self._cancel_running_job_after:
            try:
                _log.info(
                    f"Cancelling long-running job {job.job_id} (after {elapsed}, running since {job_running_start_time})"
                )
                job.stop()
            except OpenEoApiError as e:
                _log.error(f"Failed to cancel long-running job {job.job_id}: {e}")

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

    def _track_statuses(self, job_db: JobDatabaseInterface):
        """
        Tracks status (and stats) of running jobs (in place).
        Optionally cancels jobs when running too long.
        """
        active = job_db.get_by_status(statuses=["created", "queued", "running"])
        for i in active.index:
            job_id = active.loc[i, "id"]
            backend_name = active.loc[i, "backend_name"]
            previous_status = active.loc[i, "status"]

            try:
                con = self._get_connection(backend_name)
                the_job = con.job(job_id)
                job_metadata = the_job.describe()
                new_status = job_metadata["status"]

                _log.info(
                    f"Status of job {job_id!r} (on backend {backend_name}) is {new_status!r} (previously {previous_status!r})"
                )

                if new_status == "finished":
                    self.on_job_done(the_job, active.loc[i])

                if previous_status != "error" and new_status == "error":
                    self.on_job_error(the_job, active.loc[i])

                if previous_status in {"created", "queued"} and new_status == "running":
                    active.loc[i, "running_start_time"] = rfc3339.utcnow()

                if new_status == "canceled":
                    self.on_job_cancel(the_job, active.loc[i])

                if self._cancel_running_job_after and new_status == "running":
                    self._cancel_prolonged_job(the_job, active.loc[i])

                active.loc[i, "status"] = new_status

                # TODO: there is well hidden coupling here with "cpu", "memory" and "duration" from `_normalize_df`
                for key in job_metadata.get("usage", {}).keys():
                    if key in active.columns:
                        active.loc[i, key] = _format_usage_stat(job_metadata, key)

            except OpenEoApiError as e:
                print(f"error for job {job_id!r} on backend {backend_name}")
                print(e)
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

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = self.read()
        return self._df

    def count_by_status(self, statuses: List[str]) -> dict:
        status_histogram = self.df.groupby("status").size().to_dict()
        return {k:v for k,v in status_histogram.items() if k in statuses}

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
        df = pd.read_csv(self.path)
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

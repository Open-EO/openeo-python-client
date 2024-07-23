import contextlib
import datetime
import json
import logging
import time
from pathlib import Path
from typing import Callable, Dict, NamedTuple, Optional, Union

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
        max_running_duration: Optional[int] = None,
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

        :param max_running_duration [seconds]:
            A temporal limit for long running jobs to get automatically canceled.
            The preset duration 12 hours. Can be set to None to disable
        """
        self.backends: Dict[str, _Backend] = {}
        self.poll_sleep = poll_sleep
        self._connections: Dict[str, _Backend] = {}

        # An explicit None or "" should also default to "."
        self._root_dir = Path(root_dir or ".")

        self.max_running_duration = (
        datetime.timedelta(seconds=max_running_duration) if max_running_duration is not None else None
        )
     

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
        status_forcelist = [502, 503, 504]
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
        pass

        # check for some required columns.
        required_with_default = [
            ("status", "not_started"),
            ("id", None),
            ("created_timestamp", None), 
            ("running_timestamp", None), 
            # TODO: columns "cpu", "memory", "duration" are not referenced directly
            #       within MultiBackendJobManager making it confusing to claim they are required.
            #       However, they are through assumptions about job "usage" metadata in `_track_statuses`.
            ("cpu", None),
            ("memory", None),
            ("duration", None),
            ("backend_name", None)
        ]
        new_columns = {col: val for (col, val) in required_with_default if col not in df.columns}
        df = df.assign(**new_columns)

        return df

    def run_jobs(
        self,
        df: pd.DataFrame,
        start_job: Callable[[], BatchJob],
        output_file: Union[str, Path],
    ):
        """Runs jobs, specified in a dataframe, and tracks parameters.

        :param df:
            DataFrame that specifies the jobs, and tracks the jobs' statuses.

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

        :param output_file:
            Path to output file (CSV or Parquet) containing the status and metadata of the jobs.

            .. note::
                Support for Parquet files depends on the ``pyarrow`` package
                as :ref:`optional dependency <installation-optional-dependencies>`.

        .. versionchanged:: 0.31.0
            Added support for providing a Parquet ``output_file``
        """
        # TODO: Defining start_jobs as a Protocol might make its usage more clear, and avoid complicated doctrings,
        #   but Protocols are only supported in Python 3.8 and higher.
        output_file = Path(output_file)

        if output_file.suffix.lower() == ".csv":
            job_db = _CsvJobDatabase(output_file)
        elif output_file.suffix.lower() == ".parquet":
            job_db = _ParquetJobDatabase(output_file)
        else:
            raise ValueError(f"Unsupported output file type: {output_file}")

        if output_file.exists() and output_file.is_file():
            # Resume from existing db
            _log.info(f"Resuming `run_jobs` from {output_file.absolute()}")
            df = job_db.read()
            status_histogram = df.groupby("status").size().to_dict()
            _log.info(f"Status histogram: {status_histogram}")

        df = self._normalize_df(df)

        while (
            df[
                (df.status != "finished")
                & (df.status != "skipped")
                & (df.status != "start_failed")
                & (df.status != "error")
                & (df.status != "canceled")
            ].size
            > 0
        ):

            with ignore_connection_errors(context="get statuses"):
                self._track_statuses(df)
            status_histogram = df.groupby("status").size().to_dict()
            _log.info(f"Status histogram: {status_histogram}")
            job_db.persist(df)

            if len(df[df.status == "not_started"]) > 0:
                # Check number of jobs running at each backend
                running = df[(df.status == "created") | (df.status == "queued") | (df.status == "running")]
                per_backend = running.groupby("backend_name").size().to_dict()
                _log.info(f"Running per backend: {per_backend}")
                for backend_name in self.backends:
                    backend_load = per_backend.get(backend_name, 0)
                    if backend_load < self.backends[backend_name].parallel_jobs:
                        to_add = self.backends[backend_name].parallel_jobs - backend_load
                        to_launch = df[df.status == "not_started"].iloc[0:to_add]
                        for i in to_launch.index:
                            self._launch_job(start_job, df, i, backend_name)
                            job_db.persist(df)

            time.sleep(self.poll_sleep)

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
            df.loc[i, "created_timestamp"] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')  
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
        Handles jobs that that were cancelled. Can be overridden to provide custom behaviour.

        Default implementation does not do anything.

        :param job: The job that has finished.
        :param row: DataFrame row containing the job's metadata.
        """
        # TODO: param `row` is never accessed in this method. Remove it? Is this intended for future use?


    def _cancel_prolonged_job(self, job: BatchJob, row):
        """Cancel the job if it has been running for too long."""
        job_running_timestamp = rfc3339.parse_datetime(row["running_timestamp"], with_timezone = True)
        if datetime.datetime.now(datetime.timezone.utc) > job_running_timestamp + self.max_running_duration:
            try:
                job.stop()
                _log.info(
                    f"Cancelling job {job.job_id} as it has been running for more than {self.max_running_duration}"
                )
            except OpenEoApiError as e:
                _log.error(f"Error Cancelling long-running job {job.job_id}: {e}")
                
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


    def _track_statuses(self, df: pd.DataFrame):
        """tracks status (and stats) of running jobs (in place). Optinally cancels jobs when running too long"""
        active = df.loc[(df.status == "created") | (df.status == "queued") | (df.status == "running")]
        for i in active.index:
            job_id = df.loc[i, "id"]
            backend_name = df.loc[i, "backend_name"]

            try:
                con = self._get_connection(backend_name)
                the_job = con.job(job_id)
                job_metadata = the_job.describe()
                _log.info(f"Status of job {job_id!r} (on backend {backend_name}) is {job_metadata['status']!r}")

                if (df.loc[i, "status"] == "created" or df.loc[i, "status"] == "queued") and job_metadata["status"] == "running":
                    df.loc[i, "running_timestamp"] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ') 

                if self.max_running_duration and job_metadata["status"] == "running":
                    self._cancel_prolonged_job(the_job, df.loc[i])
                    
                if df.loc[i, "status"] != "error" and job_metadata["status"] == "error":
                    self.on_job_error(the_job, df.loc[i])

                if job_metadata["status"] == "finished":
                    self.on_job_done(the_job, df.loc[i])

                if job_metadata["status"] == "canceled":
                    self.on_job_cancel(the_job, df.loc[i])
                    
                df.loc[i, "status"] = job_metadata["status"]

                # TODO: there is well hidden coupling here with "cpu", "memory" and "duration" from `_normalize_df`
                for key in job_metadata.get("usage", {}).keys():
                    df.loc[i, key] = _format_usage_stat(job_metadata, key)

            except OpenEoApiError as e:
                print(f"error for job {job_id!r} on backend {backend_name}")
                print(e)


def _format_usage_stat(job_metadata: dict, field: str) -> str:
    value = deep_get(job_metadata, "usage", field, "value", default=0)
    unit = deep_get(job_metadata, "usage", field, "unit", default="")
    return f"{value} {unit}".strip()


@contextlib.contextmanager
def ignore_connection_errors(context: Optional[str] = None):
    """Context manager to ignore connection errors."""
    try:
        yield
    except requests.exceptions.ConnectionError as e:
        _log.warning(f"Ignoring connection error (context {context or 'n/a'}): {e}")
        # Back off a bit
        time.sleep(5)


class _JobDatabaseInterface:
    def read(self) -> pd.DataFrame:
        raise NotImplementedError()

    def persist(self, df: pd.DataFrame):
        raise NotImplementedError()


class _CsvJobDatabase(_JobDatabaseInterface):
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)

    def _is_valid_wkt(self, wkt: str) -> bool:
        try:
            shapely.wkt.loads(wkt)
            return True
        except shapely.errors.WKTReadingError:
            return False

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.path)
        # Workaround for loading of geopandas "geometry" column.
        if (
            "geometry" in df.columns
            and df["geometry"].dtype.name != "geometry"
            and self._is_valid_wkt(df["geometry"].iloc[0])
        ):
            df["geometry"] = df["geometry"].apply(shapely.wkt.loads)
        return df

    def persist(self, df: pd.DataFrame):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.path, index=False)


class _ParquetJobDatabase(_JobDatabaseInterface):
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)

    def read(self) -> pd.DataFrame:
        return pd.read_parquet(self.path)

    def persist(self, df: pd.DataFrame):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self.path, index=False)

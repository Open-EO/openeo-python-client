import collections
import contextlib
import datetime
import json
import logging
import time
from pathlib import Path
from typing import Callable, Dict, Optional, Union

import pandas as pd
import requests
import shapely.wkt
from requests.adapters import HTTPAdapter, Retry

from openeo import BatchJob, Connection
from openeo.rest import OpenEoApiError
from openeo.util import deep_get


_log = logging.getLogger(__name__)


# Container for backend info/settings
_Backend = collections.namedtuple("_Backend", ["get_connection", "parallel_jobs"])


MAX_RETRIES = 5

class MultiBackendJobManager:
    """
    Tracker for multiple jobs on multiple backends.

    Usage example:

    .. code-block:: python

        manager = MultiBackendJobManager()
        manager.add_backend("foo", connection=openeo.connect("http://foo.test"))
        manager.add_backend("bar", connection=openeo.connect("http://bar.test"))

        jobs_df = pd.DataFrame(...)

        output_file = Path("jobs.csv")
        def start_job(row, connection, **kwargs):
            ...

        manager.run_jobs(df=df, start_job=start_job, output_file=output_file)

    .. versionadded:: 0.14.0
    """

    def __init__(
        self, poll_sleep: int = 60, root_dir: Optional[Union[str, Path]] = "."
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
        """
        self.backends: Dict[str, _Backend] = {}
        self.poll_sleep = poll_sleep
        self._connections: Dict[str, _Backend] = {}

        # An explicit None or "" should also default to "."
        self._root_dir = Path(root_dir or ".")

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
        self.backends[name] = _Backend(
            get_connection=connection, parallel_jobs=parallel_jobs
        )

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

    def _make_resilient(self, connection):
        """Add an HTTPAdapter that retries the request if it fails.

        Retry for the following HTTP 50x statuses:
        502 Bad Gateway
        503 Service Unavailable
        504 Gateway Timeout
        """
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

        # check for some required columns.
        required_with_default = [
            ("status", "not_started"),
            ("id", None),
            ("start_time", None),
            ("cpu", None),
            ("memory", None),
            ("duration", None),
            ("backend_name", None),
        ]
        new_columns = {
            col: val for (col, val) in required_with_default if col not in df.columns
        }
        df = df.assign(**new_columns)
        # Workaround for loading of geopandas "geometry" column.
        if "geometry" in df.columns and df["geometry"].dtype.name != "geometry":
            df["geometry"] = df["geometry"].apply(shapely.wkt.loads)
        return df

    def _persists(self, df, output_file):
        df.to_csv(output_file, index=False)
        _log.info(f"Wrote job metadata to {output_file.absolute()}")

    def run_jobs(
        self, df: pd.DataFrame, start_job: Callable[[], BatchJob], output_file: Path
    ):
        """Runs jobs, specified in a dataframe, and tracks parameters.

        :param df:
            DataFrame that specifies the jobs, and tracks the jobs' statuses.

        :param start_job:
            A callback which will be invoked with the row of the dataframe for which a job should be started.
            This callable should return a :py:class:`openeo.rest.job.BatchJob` object.

            The run_jobs method passes the following parameters to the start_job callback.
            You do not have to define all of the parameters described below, but if you leave
            any of them out, then remember to include the *args and **kwargs parameters.
            Otherwise you will have an exception because run_jobs passes unknown parameters to start_job.

                row:
                    The row in the pandas dataframe that stores the jobs state and other tracked data.

                connection_provider:
                    Like connection in add_backend:
                    - either a Connection to the backend,
                    - or a callable to create a backend connection.
                    Typically you would need either the parameter `connection_provider`,
                    or the parameter `connection`, but likely you will not need both.

                connection:
                    The Connection itself, that has already been created.
                    Typically you would need either the parameter `connection_provider`,
                    or the parameter `connection`, but likely you will not need both.

                provider:
                    The name of the backend that will run the job.

        :param output_file:
            Path to output file (CSV) containing the status and metadata of the jobs.
        """
        # TODO: Defining start_jobs as a Protocol might make its usage more clear, and avoid complicated doctrings,
        #   but Protocols are only supported in Python 3.8 and higher.
        # TODO: this resume functionality better fits outside of this function
        #       (e.g. if `output_file` exists: `df` is fully discarded)

        if output_file.exists() and output_file.is_file():
            # Resume from existing CSV
            _log.info(f"Resuming `run_jobs` from {output_file.absolute()}")
            df = pd.read_csv(output_file)
            status_histogram = df.groupby("status").size().to_dict()
            _log.info(f"Status histogram: {status_histogram}")

        df = self._normalize_df(df)

        while (
            df[
                (df.status != "finished")
                & (df.status != "skipped")
                & (df.status != "start_failed")
            ].size
            > 0
        ):
            with ignore_connection_errors(context="get statuses"):
                self._update_statuses(df)
            status_histogram = df.groupby("status").size().to_dict()
            _log.info(f"Status histogram: {status_histogram}")
            self._persists(df, output_file)

            if len(df[df.status == "not_started"]) > 0:
                # Check number of jobs running at each backend
                running = df[
                    (df.status == "created")
                    | (df.status == "queued")
                    | (df.status == "running")
                ]
                per_backend = running.groupby("backend_name").size().to_dict()
                _log.info(f"Running per backend: {per_backend}")
                for backend_name in self.backends:
                    backend_load = per_backend.get(backend_name, 0)
                    if backend_load < self.backends[backend_name].parallel_jobs:
                        to_add = (
                            self.backends[backend_name].parallel_jobs - backend_load
                        )
                        to_launch = df[df.status == "not_started"].iloc[0:to_add]
                        for i in to_launch.index:
                            self._launch_job(start_job, df, i, backend_name)
                            self._persists(df, output_file)

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
            df.loc[i, "start_time"] = datetime.datetime.now().isoformat()
            if job:
                df.loc[i, "id"] = job.job_id
                with ignore_connection_errors(context="get status"):
                    status = job.status()
                    df.loc[i, "status"] = status
                    if status == "created":
                        # start job if not yet done by callback
                        try:
                            job.start_job()
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

        job_metadata = job.describe_job()
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

        logs = job.logs()
        error_logs = [l for l in logs if l.level.lower() == "error"]
        error_log_path = self.get_error_log_path(job.job_id)

        if len(error_logs) > 0:
            self.ensure_job_dir_exists(job.job_id)
            error_log_path.write_text(json.dumps(error_logs, indent=2))

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

    def _update_statuses(self, df: pd.DataFrame):
        """Update status (and stats) of running jobs (in place)."""
        active = df.loc[
            (df.status == "created")
            | (df.status == "queued")
            | (df.status == "running")
        ]
        for i in active.index:
            job_id = df.loc[i, "id"]
            backend_name = df.loc[i, "backend_name"]

            try:
                con = self._get_connection(backend_name)
                the_job = con.job(job_id)
                job_metadata = the_job.describe_job()
                _log.info(
                    f"Status of job {job_id!r} (on backend {backend_name}) is {job_metadata['status']!r}"
                )
                if (
                    df.loc[i, "status"] == "running"
                    and job_metadata["status"] == "finished"
                ):
                    self.on_job_done(the_job, df.loc[i])
                if df.loc[i, "status"] != "error" and job_metadata["status"] == "error":
                    self.on_job_error(the_job, df.loc[i])

                df.loc[i, "status"] = job_metadata["status"]
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

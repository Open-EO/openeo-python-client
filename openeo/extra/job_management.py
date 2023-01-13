import collections
import contextlib
import datetime
import json
import logging
import os
import time
from pathlib import Path
from typing import Callable, Union, Dict, Optional

import geopandas as gpd
import pandas as pd
import requests
from openeo import Connection, BatchJob
from openeo.rest import OpenEoApiError
from openeo.util import deep_get

from openeo.extra.connection import connection


_log = logging.getLogger(__name__)


def run_jobs(
    df: pd.DataFrame,
    start_job: Callable,
    outputFile: Path,
    connection_provider: Callable,
    parallel_jobs=2,
):
    """
    Runs jobs, specified in a dataframe, and tracks parameters.

    @param df: Job dataframe
    @param start_job: A callback which will be invoked with the row of the dataframe for which a job should be started.
    @param outputFile: A file on disk to track job statuses.
    @return:
    """

    # TODO: original dataframe is completely discarded if `outputFile` exists, isn't that weird?
    #       E.g. New code changes will not be picked up as long as an old/outdated CSV exist.
    if outputFile.is_file():
        df = pd.read_csv(outputFile)
        df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
    else:
        df["status"] = "not_started"
        df["start_time"] = ""
        df["id"] = "None"
        df["cpu"] = 0
        df["memory"] = 0
        df["duration"] = 0
        df.to_csv(outputFile, index=False)

    # TODO: this will never exit if there are failed/skipped jobs
    while len(df[(df["status"] != "finished")]) > 0:
        try:
            jobs_to_run = df[df.status == "not_started"]
            df = update_statuses(df, connection_provider)
            df.to_csv(outputFile, index=False)
            if jobs_to_run.empty:
                time.sleep(60)
                continue

            if (
                len(
                    df[
                        (df["status"] == "running")
                        | (df["status"] == "queued")
                        | (df["status"] == "created")
                    ]
                )
                < parallel_jobs
            ):
                next_job = jobs_to_run.iloc[0]
                job = start_job(next_job)
                if job is not None:
                    next_job["status"] = job.status()
                    next_job["id"] = job.job_id
                else:
                    next_job["status"] = "skipped"
                next_job["start_time"] = datetime.datetime.now().isoformat()
                print(next_job)
                df.loc[next_job.name] = next_job

                df.to_csv(outputFile, index=False)
            else:
                time.sleep(60)

        except requests.exceptions.ConnectionError as e:
            _log.warning(f"Skipping connection error: {e}")


def running_jobs(status_df):
    return status_df.loc[
        (status_df["status"] == "queued")
        | (status_df["status"] == "running")
        | (status_df["status"] == "created")
    ].index


def update_statuses(status_df, connection_provider=connection):
    con = connection_provider()
    for i in running_jobs(status_df):
        job_id = status_df.loc[i, "id"]
        the_job = con.job(job_id)
        job = the_job.describe_job()
        usage = job.get("usage", {})
        if status_df.loc[i, "status"] == "running" and job["status"] == "finished":
            the_job.get_results().download_files(target=job["title"])
        status_df.loc[
            i, "cpu"
        ] = f"{deep_get(usage,'cpu','value',default=0)} {deep_get(usage,'cpu','unit',default='')}"
        status_df.loc[i, "status"] = job["status"]
        status_df.loc[
            i, "memory"
        ] = f"{deep_get(usage,'memory','value',default=0)} {deep_get(usage,'memory','unit',default='')}"
        status_df.loc[i, "duration"] = deep_get(usage, "duration", "value", default=0)
        print(
            time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
            + "\tCurrent status of job "
            + job_id
            + " is : "
            + job["status"]
        )
    return status_df


# TODO: invalid path reference (https://github.com/openEOPlatform/openeo-classification/issues/2)
def create_or_load_job_statistics(path="resources/training_data/job_statistics.csv"):
    if os.path.isfile(path):
        df = pd.read_csv(path)
    else:
        df = pd.DataFrame(
            {
                "fp": [],
                "status": [],
                "start_time": [],
                "id": [],
                "cpu": [],
                "memory": [],
                "duration": [],
            }
        )
        df.to_csv(path, index=False)
    return df


# Container for backend info/settings
_Backend = collections.namedtuple("_Backend", ["get_connection", "parallel_jobs"])


class MultiBackendJobManager:
    """
    Tracker for multiple jobs on multiple backends.

    Usage example:

        manager = MultiBackendJobManager()
        manager.add_backend("foo", connection=openeo.connect("http://foo.test"))
        manager.add_backend("bar", connection=openeo.connect("http://bar.test"))

        jobs_df = pd.DataFrame(....)
        output_file = Path("jobs.csv")
        def start_job(row, connection, **kwargs):
            ...

        manager.run_jobs(df=df, start_job=start_job, output_file=output_file)

    """

    def __init__(self, poll_sleep=60):
        self.backends: Dict[str, _Backend] = {}
        self.poll_sleep = poll_sleep

    def add_backend(
        self,
        name: str,
        connection: Union[Connection, Callable[[], Connection]],
        parallel_jobs=2,
    ):
        """Register a backend with a name and a Connection getter"""
        if isinstance(connection, Connection):
            c = connection
            connection = lambda: c
        assert callable(connection)
        self.backends[name] = _Backend(
            get_connection=connection, parallel_jobs=parallel_jobs
        )

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
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
            df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
        return df

    def run_jobs(self, df: pd.DataFrame, start_job: Callable, output_file: Path):
        # TODO: this resume functionality better fits outside of this function
        #       (e.g. if `output_file` exists: `df` is fully discarded)
        if output_file.exists() and output_file.is_file():
            # Resume from existing CSV
            _log.info(f"Resuming `run_jobs` from {output_file.absolute()}")
            df = pd.read_csv(output_file)
            status_histogram = df.groupby("status").size().to_dict()
            _log.info(f"Status histogram: {status_histogram}")

        df = self._normalize_df(df)

        def persists(df, output_file):
            df.to_csv(output_file, index=False)
            _log.info(f"Wrote job metadata to {output_file.absolute()}")

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
            persists(df, output_file)

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
                            df.loc[i, "backend_name"] = backend_name
                            row = df.loc[i]
                            try:
                                _log.info(
                                    f"Starting job on backend {backend_name} for {row.to_dict()}"
                                )
                                job = start_job(
                                    row=row,
                                    connection_provider=self.backends[
                                        backend_name
                                    ].get_connection,
                                    connection=self.backends[
                                        backend_name
                                    ].get_connection(),
                                    provider=backend_name,
                                )
                            except requests.exceptions.ConnectionError as e:
                                _log.warning(
                                    f"Failed to start job for {row.to_dict()}",
                                    exc_info=True,
                                )
                                df.loc[i, "status"] = "start_failed"
                            else:
                                df.loc[
                                    i, "start_time"
                                ] = datetime.datetime.now().isoformat()
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

                            persists(df, output_file)

            time.sleep(self.poll_sleep)

    def on_job_done(self, job: BatchJob, row):
        """
        Handles jobs that have finished. Can be overridden to provide custom behaviour.

        Default implementation downloads the results into a folder containing the title.
        @param job:
        @return:
        """
        job_metadata = job.describe_job()
        job.get_results().download_files(target=job_metadata["title"])
        with open(Path(job_metadata["title"]) / f"job_{job.job_id}.json", "w") as f:
            json.dump(job_metadata, f, ensure_ascii=False)

    def on_job_error(self, job: BatchJob, row):
        logs = job.logs()
        error_logs = [l for l in logs if l.level.lower() == "error"]
        job_metadata = job.describe_job()

        title = job_metadata["title"]
        if len(error_logs) > 0:
            (f"job_{title}_errors.json").write_text(json.dumps(error_logs, indent=2))

    def _update_statuses(self, df: pd.DataFrame):
        """Update status (and stats) of running jobs (in place)"""
        active = df.loc[
            (df.status == "created")
            | (df.status == "queued")
            | (df.status == "running")
        ]
        for i in active.index:
            job_id = df.loc[i, "id"]
            backend_name = df.loc[i, "backend_name"]

            try:
                con = self.backends[backend_name].get_connection()
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
                print(f"error for {backend_name}")
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

import collections
import copy
import datetime
import json
import logging
import re
import threading
import time
from pathlib import Path
from time import sleep
from typing import Union
from unittest import mock

import dirty_equals
import geopandas

# TODO: can we avoid using httpretty?
#   We need it for testing the resilience, which uses an HTTPadapter with Retry
#   but requests-mock also uses an HTTPAdapter for the mocking and basically
#   erases the HTTPAdapter we have set up.
#   httpretty avoids this specific problem because it mocks at the socket level,
#   But I would rather not have two dependencies with almost the same goal.
import httpretty
import numpy as np
import pandas
import pandas as pd
import pytest
import requests
import shapely.geometry

import openeo
import openeo.extra.job_management
from openeo import BatchJob
from openeo.extra.job_management import (
    MAX_RETRIES,
    CsvJobDatabase,
    MultiBackendJobManager,
    ParquetJobDatabase,
    ProcessBasedJobCreator,
    create_job_db,
    get_job_db,
)
from openeo.extra.job_management._thread_worker import (
    Task,
    _JobManagerWorkerThreadPool,
    _TaskResult,
)
from openeo.rest._testing import OPENEO_BACKEND, DummyBackend, build_capabilities
from openeo.util import rfc3339
from openeo.utils.version import ComparableVersion


@pytest.fixture
def con(requests_mock) -> openeo.Connection:
    requests_mock.get(OPENEO_BACKEND, json=build_capabilities(api_version="1.2.0", udp=True))
    con = openeo.Connection(OPENEO_BACKEND)
    return con


def _job_id_from_year(process_graph) -> Union[str, None]:
    """Job id generator that extracts the year from the process graph"""
    try:
        (year,) = (n["arguments"]["year"] for n in process_graph.values())
        return f"job-{year}"
    except Exception:
        pass


@pytest.fixture
def dummy_backend_foo(requests_mock) -> DummyBackend:
    dummy = DummyBackend.at_url("https://foo.test", requests_mock=requests_mock)
    dummy.setup_simple_job_status_flow(queued=3, running=5)
    dummy.job_id_generator = _job_id_from_year
    return dummy


@pytest.fixture
def dummy_backend_bar(requests_mock) -> DummyBackend:
    dummy = DummyBackend.at_url("https://bar.test", requests_mock=requests_mock)
    dummy.setup_simple_job_status_flow(queued=5, running=8)
    dummy.job_id_generator = _job_id_from_year
    return dummy


@pytest.fixture
def sleep_mock():
    with mock.patch("time.sleep") as sleep:
        yield sleep


class DummyTask(Task):
    """
    A Task that simply sleeps and then returns a predetermined _TaskResult.
    """

    def __init__(self, job_id, df_idx, db_update, stats_update):
        super().__init__(job_id=job_id, df_idx=df_idx)
        self._db_update = db_update or {}
        self._stats_update = stats_update or {}

    def execute(self) -> _TaskResult:
        return _TaskResult(
            job_id=self.job_id,
            df_idx=self.df_idx,
            db_update=self._db_update,
            stats_update=self._stats_update,
        )


class TestMultiBackendJobManager:

    @pytest.fixture
    def job_manager_root_dir(self, tmp_path):
        return tmp_path / "job_mgr_root"

    @pytest.fixture
    def job_manager(self, job_manager_root_dir, dummy_backend_foo, dummy_backend_bar):
        manager = MultiBackendJobManager(root_dir=job_manager_root_dir)
        manager.add_backend("foo", connection=dummy_backend_foo.connection)
        manager.add_backend("bar", connection=dummy_backend_bar.connection)
        return manager

    @staticmethod
    def _create_year_job(row, connection, **kwargs):
        """Job creation callable to use with MultiBackendJobManager run_jobs"""
        year = int(row["year"])
        pg = {"yearify": {"process_id": "yearify", "arguments": {"year": year}, "result": True}}
        return connection.create_job(pg)

    def test_basic_legacy(self, tmp_path, job_manager, job_manager_root_dir, sleep_mock):
        """
        Legacy `run_jobs()` usage with explicit dataframe and output file
        """
        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        job_db_path = tmp_path / "jobs.csv"

        run_stats = job_manager.run_jobs(df=df, start_job=self._create_year_job, output_file=job_db_path)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=10),
                "start_job call": 5,
                "job started running": 5,
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
                "run_jobs loop": dirty_equals.IsInt(gt=5),
            }
        )

        assert [(r.id, r.status, r.backend_name) for r in pd.read_csv(job_db_path).itertuples()] == [
            ("job-2018", "finished", "foo"),
            ("job-2019", "finished", "foo"),
            ("job-2020", "finished", "bar"),
            ("job-2021", "finished", "bar"),
            ("job-2022", "finished", "foo"),
        ]

        # Check downloaded results and metadata.
        assert set(p.relative_to(job_manager_root_dir) for p in job_manager_root_dir.glob("**/*.*")) == {
            Path(f"job_{job_id}") / filename
            for job_id in ["job-2018", "job-2019", "job-2020", "job-2021", "job-2022"]
            for filename in ["job-results.json", f"job_{job_id}.json", "result.data"]
        }

    def test_basic(self, tmp_path, job_manager, job_manager_root_dir, sleep_mock):
        """
        `run_jobs()` usage with a `CsvJobDatabase`
        (and no explicit dataframe or output file)
        """
        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        job_db_path = tmp_path / "jobs.csv"

        job_db = CsvJobDatabase(job_db_path).initialize_from_df(df)
        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=10),
                "start_job call": 5,
                "job started running": 5,
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
                "run_jobs loop": dirty_equals.IsInt(gt=5),
            }
        )

        assert [
            (r.id, r.status, r.backend_name, r.cpu, r.memory, r.duration, r.costs)
            for r in pd.read_csv(job_db_path).itertuples()
        ] == [
            ("job-2018", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2019", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2020", "finished", "bar", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2021", "finished", "bar", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2022", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
        ]

        # Check downloaded results and metadata.
        assert set(p.relative_to(job_manager_root_dir) for p in job_manager_root_dir.glob("**/*.*")) == {
            Path(f"job_{job_id}") / filename
            for job_id in ["job-2018", "job-2019", "job-2020", "job-2021", "job-2022"]
            for filename in ["job-results.json", f"job_{job_id}.json", "result.data"]
        }

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_db_class(self, tmp_path, job_manager, job_manager_root_dir, sleep_mock, db_class):
        """
        Basic run parameterized on database class
        """

        df = pd.DataFrame({"year": [2018, 2019, 2020, 2021, 2022]})
        output_file = tmp_path / "jobs.db"
        job_db = db_class(output_file).initialize_from_df(df)

        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 5,
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
            }
        )

        result = job_db.read()
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}
        assert set(result.cpu) == {"1234.5 cpu-seconds"}
        assert set(result.memory) == {"34567.89 mb-seconds"}
        assert set(result.duration) == {"2345 seconds"}
        assert set(result.costs) == {123}

    @pytest.mark.parametrize(
        ["filename", "expected_db_class"],
        [
            ("jobz.csv", CsvJobDatabase),
            ("jobz.parquet", ParquetJobDatabase),
        ],
    )
    def test_create_job_db(self, tmp_path, job_manager, job_manager_root_dir, sleep_mock, filename, expected_db_class):
        """
        Basic run with `create_job_db()` usage
        """

        df = pd.DataFrame({"year": [2018, 2019, 2020, 2021, 2022]})
        output_file = tmp_path / filename
        job_db = create_job_db(path=output_file, df=df)

        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 5,
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
            }
        )

        result = job_db.read()
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

    def test_basic_threading(self, tmp_path, job_manager, job_manager_root_dir, sleep_mock):
        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        job_db_path = tmp_path / "jobs.csv"

        job_db = CsvJobDatabase(job_db_path).initialize_from_df(df)

        job_manager.start_job_thread(start_job=self._create_year_job, job_db=job_db)
        # Trigger context switch to job thread
        sleep(1)
        job_manager.stop_job_thread()
        # TODO #645 how to collect stats with the threaded run_job?
        assert sleep_mock.call_count > 10

        assert [
            (r.id, r.status, r.backend_name, r.cpu, r.memory, r.duration, r.costs)
            for r in pd.read_csv(job_db_path).itertuples()
        ] == [
            ("job-2018", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2019", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2020", "finished", "bar", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2021", "finished", "bar", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2022", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
        ]

        # Check downloaded results and metadata.
        assert set(p.relative_to(job_manager_root_dir) for p in job_manager_root_dir.glob("**/*.*")) == {
            Path(f"job_{job_id}") / filename
            for job_id in ["job-2018", "job-2019", "job-2020", "job-2021", "job-2022"]
            for filename in ["job-results.json", f"job_{job_id}.json", "result.data"]
        }

    def test_normalize_df(self):
        df = pd.DataFrame({"some_number": [3, 2, 1]})
        df_normalized = MultiBackendJobManager._normalize_df(df)
        assert set(df_normalized.columns) == set(
            [
                "some_number",
                "status",
                "id",
                "start_time",
                "running_start_time",
                "cpu",
                "memory",
                "duration",
                "backend_name",
                "costs",
            ]
        )

    def test_manager_must_exit_when_all_jobs_done(
        self, tmp_path, sleep_mock, job_manager, job_manager_root_dir, dummy_backend_foo, dummy_backend_bar
    ):
        """Make sure the MultiBackendJobManager does not hang after all processes finish.

        Regression test for:
        https://github.com/Open-EO/openeo-python-client/issues/432

        Cause was that the run_jobs had an infinite loop when jobs ended with status error.
        """

        dummy_backend_foo.setup_simple_job_status_flow(
            queued=2, running=3, final="finished", final_per_job={"job-2022": "error"}
        )
        dummy_backend_bar.setup_simple_job_status_flow(
            queued=2, running=3, final="finished", final_per_job={"job-2022": "error"}
        )

        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        job_db_path = tmp_path / "jobs.csv"
        job_db = CsvJobDatabase(job_db_path).initialize_from_df(df)

        is_done_file = tmp_path / "is_done.txt"

        def start_worker_thread():
            job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)
            is_done_file.write_text("Done!")

        thread = threading.Thread(target=start_worker_thread, name="Worker process", daemon=True)

        timeout_sec = 5.0
        thread.start()
        # We stop waiting for the process after the timeout.
        # If that happens it is likely we detected that run_jobs will loop infinitely.
        thread.join(timeout=timeout_sec)

        assert is_done_file.exists(), (
            "MultiBackendJobManager did not finish on its own and was killed. " + "Infinite loop is probable."
        )

        # Also check that we got sensible end results in the job db.
        results = pd.read_csv(job_db_path).replace({np.nan: None})  # np.nan's are replaced by None for easy comparison
        assert [
            (r.id, r.status, r.backend_name, r.cpu, r.memory, r.duration, r.costs) for r in results.itertuples()
        ] == [
            ("job-2018", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2019", "finished", "foo", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2020", "finished", "bar", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2021", "finished", "bar", "1234.5 cpu-seconds", "34567.89 mb-seconds", "2345 seconds", 123),
            ("job-2022", "error", "foo", None, None, None, None),
        ]

        # Check downloaded results and metadata.
        assert set(p.relative_to(job_manager_root_dir) for p in job_manager_root_dir.glob("**/*.*")) == {
            Path(f"job_{job_id}") / filename
            for job_id in ["job-2018", "job-2019", "job-2020", "job-2021"]
            for filename in ["job-results.json", f"job_{job_id}.json", "result.data"]
        }

    def test_on_error_log(self, tmp_path, requests_mock):
        backend = "http://foo.test"
        requests_mock.get(backend, json={"api_version": "1.1.0"})

        job_id = "job-2018"
        errors_log_lines = [
            {
                "id": job_id,
                "level": "error",
                "message": "Test that error handling works",
            }
        ]
        requests_mock.get(f"{backend}/jobs/{job_id}/logs", json={"logs": errors_log_lines})

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(root_dir=root_dir)
        connection = openeo.connect(backend)
        manager.add_backend("foo", connection=connection)

        df = pd.DataFrame({"year": [2018]})
        job = BatchJob(job_id=f"job-2018", connection=connection)
        row = df.loc[0]

        manager.on_job_error(job=job, row=row)

        # Check that the error log file exists and contains the message we expect.
        error_log_path = manager.get_error_log_path(job_id=job_id)
        assert error_log_path.exists()
        contents = error_log_path.read_text()
        assert json.loads(contents) == errors_log_lines

    @httpretty.activate(allow_net_connect=False, verbose=True)
    @pytest.mark.parametrize("http_error_status", [502, 503, 504])
    def test_is_resilient_to_backend_failures(self, tmp_path, http_error_status, sleep_mock):
        """
        Our job should still succeed when the backend request succeeds eventually,
        after first failing the maximum allowed number of retries.

        Goal of the test is only to see that retrying is effectively executed.

        But we don't care much about the details of the retrying (config),
        because that would really be testing stuff that the requests library already checks.

        Nota bene:

        This test needs httpretty instead of requests_mock because the requests_mock uses
        an HTTPAdapter for its mocking, and that overrides the HTTPAdaptor we are adding
        for the retry behavior.
        """

        backend = "http://foo.test"
        job_id = "job-2018"

        httpretty.register_uri("GET", backend, body=json.dumps({"api_version": "1.1.0"}))

        # First fail the max times the connection should retry, then succeed. after that
        response_list = [
            httpretty.Response(f"Simulate error HTTP {http_error_status}", status=http_error_status)
        ] * MAX_RETRIES
        response_list += [
            httpretty.Response(
                body=json.dumps(
                    {
                        "id": job_id,
                        "title": f"Job {job_id}",
                        "status": "finished",
                    }
                )
            )
        ]
        httpretty.register_uri("GET", f"{backend}/jobs/{job_id}", responses=response_list)

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(root_dir=root_dir)
        connection = openeo.connect(backend)
        manager.add_backend("foo", connection=connection)

        df = pd.DataFrame(
            {
                "year": [2018],
            }
        )

        def start_job(row, connection_provider, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        job_db_path = tmp_path / "jobs.csv"

        run_stats = manager.run_jobs(df=df, start_job=start_job, output_file=job_db_path)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 1,
            }
        )

        # Sanity check: the job succeeded
        assert [(r.id, r.status, r.backend_name) for r in pd.read_csv(job_db_path).itertuples()] == [
            ("job-2018", "finished", "foo"),
        ]

    @httpretty.activate(allow_net_connect=False, verbose=True)
    @pytest.mark.parametrize("http_error_status", [502, 503, 504])
    def test_resilient_backend_reports_error_when_max_retries_exceeded(self, tmp_path, http_error_status, sleep_mock):
        """We should get a RetryError when the backend request fails more times than the maximum allowed number of retries.

        Goal of the test is only to see that retrying is effectively executed.

        But we don't care much about the details of the retrying (config),
        because that would really be testing stuff that the requests library already checks.

        Nota bene:

        This test needs httpretty instead of requests_mock because the requests_mock uses
        an HTTPAdapter for its mocking, and that overrides the HTTPAdaptor we are adding
        for the retry behavior.
        """

        backend = "http://foo.test"
        job_id = "job-2018"

        httpretty.register_uri("GET", backend, body=json.dumps({"api_version": "1.1.0"}))

        # Fail one more time than the max allow retries.
        # But do add one successful request at the start, to simulate that the job was
        # in running mode at one point.
        # Namely, we want to check that it flags the job stopped with an error.
        response_list = [
            httpretty.Response(
                body=json.dumps(
                    {
                        "id": job_id,
                        "title": f"Job {job_id}",
                        "status": "running",
                    }
                )
            )
        ]
        response_list += [httpretty.Response(f"Simulate error HTTP {http_error_status}", status=http_error_status)] * (
            MAX_RETRIES + 1
        )

        httpretty.register_uri("GET", f"{backend}/jobs/{job_id}", responses=response_list)

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(root_dir=root_dir)
        connection = openeo.connect(backend)
        manager.add_backend("foo", connection=connection)

        df = pd.DataFrame(
            {
                "year": [2018],
            }
        )

        def start_job(row, connection_provider, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        job_db_path = tmp_path / "jobs.csv"

        with pytest.raises(requests.exceptions.RetryError) as exc:
            manager.run_jobs(df=df, start_job=start_job, output_file=job_db_path)

        # TODO #645 how to still check stats when run_jobs raised exception?
        assert sleep_mock.call_count > 3

        # Sanity check: the job has status "error"
        assert [(r.id, r.status, r.backend_name) for r in pd.read_csv(job_db_path).itertuples()] == [
            ("job-2018", "running", "foo"),
        ]

    @pytest.mark.parametrize(
        ["create_time", "start_time", "end_time", "end_status", "cancel_after_seconds", "expected_status"],
        [
            (
                "2024-09-01T9:00:00Z",
                "2024-09-01T10:00:00Z",
                "2024-09-01T20:00:00Z",
                "finished",
                6 * 60 * 60,
                "canceled",
            ),
            (
                "2024-09-01T09:00:00Z",
                "2024-09-01T10:00:00Z",
                "2024-09-01T20:00:00Z",
                "finished",
                12 * 60 * 60,
                "finished",
            ),

        ],
    )
    def test_automatic_cancel_of_too_long_running_jobs(
        self,
        tmp_path,
        time_machine,
        create_time,
        start_time,
        end_time,
        end_status,
        cancel_after_seconds,
        expected_status,
        dummy_backend_foo,
        job_manager_root_dir,
    ):
        def get_status(job_id, current_status):
            if rfc3339.now_utc() < start_time:
                return "queued"
            elif rfc3339.now_utc() < end_time:
                return "running"
            return end_status

        dummy_backend_foo.job_status_updater = get_status

        job_manager = MultiBackendJobManager(
            root_dir=job_manager_root_dir, cancel_running_job_after=cancel_after_seconds
        )
        job_manager.add_backend("foo", connection=dummy_backend_foo.connection)

        df = pd.DataFrame({"year": [2024]})

        time_machine.move_to(create_time)
        job_db_path = tmp_path / "jobs.csv"

        # Mock sleep() to not actually sleep, but skip one hour at a time
        with mock.patch.object(openeo.extra.job_management.time, "sleep", new=lambda s: time_machine.shift(60 * 60)):
            job_manager.run_jobs(df=df, start_job=self._create_year_job, job_db=job_db_path)

        final_df = CsvJobDatabase(job_db_path).read()
        assert dirty_equals.IsPartialDict(id="job-2024", status=expected_status) == final_df.iloc[0].to_dict()

        assert dummy_backend_foo.batch_jobs == {
            "job-2024": {
                "job_id": "job-2024",
                "pg": {"yearify": {"process_id": "yearify", "arguments": {"year": 2024}, "result": True}},
                "status": expected_status,
            }
        }

    def test_empty_csv_handling(self, tmp_path, sleep_mock, recwarn, job_manager):
        """
        Check how starting from an empty CSV is handled:
        will empty columns accepts string values without warning/error?
        """
        df = pd.DataFrame({"year": [2021, 2022]})

        job_db_path = tmp_path / "jobs.csv"
        # Initialize job db and trigger writing it to CSV file
        _ = CsvJobDatabase(job_db_path).initialize_from_df(df)

        assert job_db_path.exists()
        # Simple check for empty columns in the CSV file
        assert ",,,,," in job_db_path.read_text()

        # Start over with existing file
        job_db = CsvJobDatabase(job_db_path)

        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)
        assert run_stats == dirty_equals.IsPartialDict({"start_job call": 2, "job finished": 2})

        assert [(r.id, r.status) for r in pd.read_csv(job_db_path).itertuples()] == [
            ("job-2021", "finished"),
            ("job-2022", "finished"),
        ]

        assert [(w.category, w.message, str(w)) for w in recwarn.list] == []

    def test_status_logging(self, tmp_path, job_manager, job_manager_root_dir, sleep_mock, caplog):
        caplog.set_level(logging.INFO)
        df = pd.DataFrame({"year": [2018, 2019, 2020, 2021, 2022]})
        job_db_path = tmp_path / "jobs.csv"
        job_db = CsvJobDatabase(job_db_path).initialize_from_df(df)

        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)
        assert run_stats == dirty_equals.IsPartialDict({"start_job call": 5, "job finished": 5})

        needle = re.compile(r"Job status histogram:.*'finished': 5.*Run stats:.*'job_queued_for_start': 5")
        assert needle.search(caplog.text)



    @pytest.mark.parametrize(
    ["create_time", "start_time", "running_start_time", "end_time", "end_status", "cancel_after_seconds"],
    [
        # Scenario 1: Missing running_start_time (None)
        (
            "2024-09-01T09:00:00Z",  # Job creation time
            "2024-09-01T09:00:00Z",  # Job start time (should be 1 hour after create_time)
            None,                     # Missing running_start_time
            "2024-09-01T20:00:00Z",  # Job end time
            "finished",               # Job final status
            6 * 60 * 60,              # Cancel after 6 hours
        ),
        # Scenario 2: NaN running_start_time
        (
            "2024-09-01T09:00:00Z",
            "2024-09-01T09:00:00Z",
            float("nan"),             # NaN running_start_time
            "2024-09-01T20:00:00Z",  # Job end time
            "finished",               # Job final status
            6 * 60 * 60,              # Cancel after 6 hours
        ),
    ]
    )
    def test_ensure_running_start_time_is_datetime(
        self,
        tmp_path,
        time_machine,
        create_time,
        start_time,
        running_start_time,
        end_time,
        end_status,
        cancel_after_seconds,
        dummy_backend_foo,
        job_manager_root_dir,
    ):
        def get_status(job_id, current_status):
            if rfc3339.now_utc() < start_time:
                return "queued"
            elif rfc3339.now_utc() < end_time:
                return "running"
            return end_status

        # Set the job status updater function for the mock backend
        dummy_backend_foo.job_status_updater = get_status

        job_manager = MultiBackendJobManager(
            root_dir=job_manager_root_dir, cancel_running_job_after=cancel_after_seconds
        )
        job_manager.add_backend("foo", connection=dummy_backend_foo.connection)

        # Create a DataFrame representing the job database
        df = pd.DataFrame({
            "year": [2024],
            "running_start_time": [running_start_time],  # Initial running_start_time
        })

        # Move the time machine to the job creation time
        time_machine.move_to(create_time)

        job_db_path = tmp_path / "jobs.csv"

        # Mock sleep() to skip one hour at a time instead of actually sleeping
        with mock.patch.object(openeo.extra.job_management.time, "sleep", new=lambda s: time_machine.shift(60 * 60)):
            job_manager.run_jobs(df=df, start_job=self._create_year_job, job_db=job_db_path)

        final_df = CsvJobDatabase(job_db_path).read()

        # Validate running_start_time is a valid datetime object
        filled_running_start_time = final_df.iloc[0]["running_start_time"]
        assert isinstance(rfc3339.parse_datetime(filled_running_start_time), datetime.datetime)


    def test_process_threadworker_updates(self, tmp_path, caplog):
        pool = _JobManagerWorkerThreadPool(max_workers=2)
        stats = collections.defaultdict(int)

        # Submit tasks covering all cases
        pool.submit_task(DummyTask("j-0", df_idx=0, db_update={"status": "queued"}, stats_update={"queued": 1}))
        pool.submit_task(DummyTask("j-1", df_idx=1, db_update={"status": "queued"}, stats_update=None))
        pool.submit_task(DummyTask("j-2", df_idx=2, db_update=None, stats_update={"queued": 1}))
        pool.submit_task(DummyTask("j-3", df_idx=3, db_update=None, stats_update=None))
        # Invalid index (not in DB)
        pool.submit_task(DummyTask("j-missing", df_idx=4, db_update={"status": "created"}, stats_update=None))

        df_initial = pd.DataFrame(
            {
                "id": ["j-0", "j-1", "j-2", "j-3"],
                "status": ["created", "created", "created", "created"],
            }
        )
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df_initial)

        mgr = MultiBackendJobManager(root_dir=tmp_path / "jobs")

        with caplog.at_level(logging.ERROR):
            mgr._process_threadworker_updates(worker_pool=pool, job_db=job_db, stats=stats)

        df_final = job_db.read()

        # Assert no rows were appended
        assert len(df_final) == 4

        # Assert updates
        assert df_final.loc[0, "status"] == "queued"
        assert df_final.loc[1, "status"] == "queued"
        assert df_final.loc[2, "status"] == "created"
        assert df_final.loc[3, "status"] == "created"

        # Assert stats
        assert stats.get("queued", 0) == 2
        assert stats["job_db persist"] == 1

    def test_no_results_leaves_db_and_stats_untouched(self, tmp_path, caplog):
        pool = _JobManagerWorkerThreadPool(max_workers=2)
        stats = collections.defaultdict(int)

        df_initial = pd.DataFrame({"id": ["j-0"], "status": ["created"]})
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df_initial)
        mgr = MultiBackendJobManager(root_dir=tmp_path / "jobs")

        mgr._process_threadworker_updates(pool, job_db=job_db, stats=stats)

        df_final = job_db.read()
        assert df_final.loc[0, "status"] == "created"
        assert stats == {}

    def test_logs_on_invalid_update(self, tmp_path, caplog):
        pool = _JobManagerWorkerThreadPool(max_workers=2)
        stats = collections.defaultdict(int)

        # Malformed db_update (not a dict unpackable via **)
        class BadTask:
            job_id = "bad-task"
            df_idx = 0
            db_update = "invalid"  # invalid
            stats_update = "a"

            def execute(self):
                return self

        pool.submit_task(BadTask())

        df_initial = pd.DataFrame({"id": ["j-0"], "status": ["created"]})
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df_initial)
        mgr = MultiBackendJobManager(root_dir=tmp_path / "jobs")

        with caplog.at_level(logging.ERROR):
            mgr._process_threadworker_updates(pool, job_db=job_db, stats=stats)

        # DB should remain unchanged
        df_final = job_db.read()
        assert df_final.loc[0, "status"] == "created"

        # Stats remain empty
        assert stats == {}

        # Assert log about invalid db update
        assert any("Skipping invalid db_update" in msg for msg in caplog.messages)
        assert any("Skipping invalid stats_update" in msg for msg in caplog.messages)

JOB_DB_DF_BASICS = pd.DataFrame(
    {
        "numbers": [3, 2, 1],
        "names": ["apple", "banana", "coconut"],
    }
)
JOB_DB_GDF_WITH_GEOMETRY = geopandas.GeoDataFrame(
    {
        "numbers": [11, 22],
        "geometry": [shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 1)],
    },
)
JOB_DB_DF_WITH_GEOJSON_STRING = pd.DataFrame(
    {
        "numbers": [11, 22],
        "geometry": ['{"type":"Point","coordinates":[1,2]}', '{"type":"Point","coordinates":[1,2]}'],
    }
)


class TestFullDataFrameJobDatabase:
    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_initialize_from_df(self, tmp_path, db_class):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.db"

        db = db_class(path)
        assert not path.exists()
        db.initialize_from_df(orig_df)
        assert path.exists()

        # Check persisted CSV
        assert path.exists()
        expected_columns = {
            "some_number",
            "status",
            "id",
            "start_time",
            "running_start_time",
            "cpu",
            "memory",
            "duration",
            "backend_name",
            "costs",
        }

        actual_columns = set(db_class(path).read().columns)
        assert actual_columns == expected_columns

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_initialize_from_df_on_exists_error(self, tmp_path, db_class):
        df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.csv"
        _ = db_class(path).initialize_from_df(df, on_exists="error")
        assert path.exists()

        with pytest.raises(FileExistsError, match="Job database.* already exists"):
            _ = db_class(path).initialize_from_df(df, on_exists="error")

        assert set(db_class(path).read()["some_number"]) == {1, 2, 3}

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_initialize_from_df_on_exists_skip(self, tmp_path, db_class):
        path = tmp_path / "jobs.db"

        db = db_class(path).initialize_from_df(
            pd.DataFrame({"some_number": [3, 2, 1]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

        db = db_class(path).initialize_from_df(
            pd.DataFrame({"some_number": [444, 555, 666]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_count_by_status(self, tmp_path, db_class):
        path = tmp_path / "jobs.db"

        db = db_class(path).initialize_from_df(
            pd.DataFrame(
                {
                    "status": [
                        "not_started",
                        "created",
                        "queued",
                        "queued",
                        "queued",
                        "running",
                        "running",
                        "finished",
                        "finished",
                        "error",
                    ]
                }
            )
        )
        assert db.count_by_status(statuses=["not_started"]) == {"not_started": 1}
        assert db.count_by_status(statuses=("not_started", "running")) == {"not_started": 1, "running": 2}
        assert db.count_by_status(statuses={"finished", "error"}) == {"error": 1, "finished": 2}

        # All statuses by default
        assert db.count_by_status() == {
            "created": 1,
            "error": 1,
            "finished": 2,
            "not_started": 1,
            "queued": 3,
            "running": 2,
        }


class TestCsvJobDatabase:

    def test_repr(self, tmp_path):
        path = tmp_path / "db.csv"
        db = CsvJobDatabase(path)
        assert re.match(r"CsvJobDatabase\('[^']+\.csv'\)", repr(db))
        assert re.match(r"CsvJobDatabase\('[^']+\.csv'\)", str(db))

    def test_read_wkt(self, tmp_path):
        wkt_df = pd.DataFrame(
            {
                "value": ["wkt"],
                "geometry": ["POINT (30 10)"],
            }
        )
        path = tmp_path / "jobs.csv"
        wkt_df.to_csv(path, index=False)
        df = CsvJobDatabase(path).read()
        assert isinstance(df.geometry[0], shapely.geometry.Point)

    def test_read_non_wkt(self, tmp_path):
        non_wkt_df = pd.DataFrame(
            {
                "value": ["non_wkt"],
                "geometry": ["this is no WKT"],
            }
        )
        path = tmp_path / "jobs.csv"
        non_wkt_df.to_csv(path, index=False)
        df = CsvJobDatabase(path).read()
        assert isinstance(df.geometry[0], str)

    @pytest.mark.parametrize(
        ["orig"],
        [
            pytest.param(JOB_DB_DF_BASICS, id="pandas basics"),
            pytest.param(JOB_DB_GDF_WITH_GEOMETRY, id="geopandas with geometry"),
            pytest.param(JOB_DB_DF_WITH_GEOJSON_STRING, id="pandas with geojson string as geometry"),
        ],
    )
    def test_persist_and_read(self, tmp_path, orig: pandas.DataFrame):
        path = tmp_path / "jobs.parquet"
        CsvJobDatabase(path).persist(orig)
        assert path.exists()

        loaded = CsvJobDatabase(path).read()
        assert loaded.dtypes.to_dict() == orig.dtypes.to_dict()
        assert loaded.equals(orig)
        assert type(orig) is type(loaded)

    @pytest.mark.parametrize(
        ["orig"],
        [
            pytest.param(JOB_DB_DF_BASICS, id="pandas basics"),
            pytest.param(JOB_DB_GDF_WITH_GEOMETRY, id="geopandas with geometry"),
            pytest.param(JOB_DB_DF_WITH_GEOJSON_STRING, id="pandas with geojson string as geometry"),
        ],
    )
    def test_partial_read_write(self, tmp_path, orig: pandas.DataFrame):
        path = tmp_path / "jobs.csv"

        required_with_default = [
            ("status", "not_started"),
            ("id", None),
            ("start_time", None),
        ]
        new_columns = {col: val for (col, val) in required_with_default if col not in orig.columns}
        orig = orig.assign(**new_columns)

        db = CsvJobDatabase(path)
        db.persist(orig)
        assert path.exists()

        loaded = db.get_by_status(statuses=["not_started"], max=2)
        assert db.count_by_status(statuses=["not_started"])["not_started"] > 1

        assert len(loaded) == 2
        loaded.loc[0, "status"] = "running"
        loaded.loc[1, "status"] = "error"
        db.persist(loaded)
        assert db.count_by_status(statuses=["error"])["error"] == 1

        all = db.read()
        assert len(all) == len(orig)
        assert all.loc[0, "status"] == "running"
        assert all.loc[1, "status"] == "error"
        if len(all) > 2:
            assert all.loc[2, "status"] == "not_started"
        print(loaded.index)

    def test_initialize_from_df(self, tmp_path):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.csv"

        # Initialize the CSV from the dataframe
        _ = CsvJobDatabase(path).initialize_from_df(orig_df)

        # Check persisted CSV
        assert path.exists()
        expected_columns = {
            "some_number",
            "status",
            "id",
            "start_time",
            "running_start_time",
            "cpu",
            "memory",
            "duration",
            "backend_name",
            "costs",
        }

        # Raw file content check
        raw_columns = set(path.read_text().split("\n")[0].split(","))
        # Higher level read
        read_columns = set(CsvJobDatabase(path).read().columns)

        assert raw_columns == expected_columns
        assert read_columns == expected_columns

    def test_initialize_from_df_on_exists_error(self, tmp_path):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.csv"
        _ = CsvJobDatabase(path).initialize_from_df(orig_df, on_exists="error")
        with pytest.raises(FileExistsError, match="Job database.* already exists"):
            _ = CsvJobDatabase(path).initialize_from_df(orig_df, on_exists="error")

    def test_initialize_from_df_on_exists_skip(self, tmp_path):
        path = tmp_path / "jobs.csv"

        db = CsvJobDatabase(path).initialize_from_df(
            pd.DataFrame({"some_number": [3, 2, 1]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

        db = CsvJobDatabase(path).initialize_from_df(
            pd.DataFrame({"some_number": [444, 555, 666]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

    @pytest.mark.skipif(
        ComparableVersion(geopandas.__version__) < "0.14",
        reason="This issue has no workaround with geopandas < 0.14 (highest available version on Python 3.8 is 0.13.2)",
    )
    def test_read_with_crs_column(self, tmp_path):
        """
        Having a column named "crs" can cause obscure error messages when creating a GeoPandas dataframe
        https://github.com/Open-EO/openeo-python-client/issues/714
        """
        source_df = pd.DataFrame(
            {
                "crs": [1234],
                "geometry": ["Point(2 3)"],
            }
        )
        path = tmp_path / "jobs.csv"
        source_df.to_csv(path, index=False)
        result_df = CsvJobDatabase(path).read()
        assert isinstance(result_df, geopandas.GeoDataFrame)
        assert result_df.to_dict(orient="list") == {
            "crs": [1234],
            "geometry": [shapely.geometry.Point(2, 3)],
        }


class TestParquetJobDatabase:

    def test_repr(self, tmp_path):
        path = tmp_path / "db.pq"
        db = ParquetJobDatabase(path)
        assert re.match(r"ParquetJobDatabase\('[^']+\.pq'\)", repr(db))
        assert re.match(r"ParquetJobDatabase\('[^']+\.pq'\)", str(db))

    @pytest.mark.parametrize(
        ["orig"],
        [
            pytest.param(JOB_DB_DF_BASICS, id="pandas basics"),
            pytest.param(JOB_DB_GDF_WITH_GEOMETRY, id="geopandas with geometry"),
            pytest.param(JOB_DB_DF_WITH_GEOJSON_STRING, id="pandas with geojson string as geometry"),
        ],
    )
    def test_persist_and_read(self, tmp_path, orig: pandas.DataFrame):
        path = tmp_path / "jobs.parquet"
        ParquetJobDatabase(path).persist(orig)
        assert path.exists()

        loaded = ParquetJobDatabase(path).read()
        assert loaded.dtypes.to_dict() == orig.dtypes.to_dict()
        assert loaded.equals(orig)
        assert type(orig) is type(loaded)

    def test_initialize_from_df(self, tmp_path):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.parquet"

        # Initialize the CSV from the dataframe
        _ = ParquetJobDatabase(path).initialize_from_df(orig_df)

        # Check persisted CSV
        assert path.exists()
        expected_columns = {
            "some_number",
            "status",
            "id",
            "start_time",
            "running_start_time",
            "cpu",
            "memory",
            "duration",
            "backend_name",
            "costs",
        }

        df_from_disk = ParquetJobDatabase(path).read()
        assert set(df_from_disk.columns) == expected_columns


@pytest.mark.parametrize(
    ["filename", "expected"],
    [
        ("jobz.csv", CsvJobDatabase),
        ("jobz.parquet", ParquetJobDatabase),
    ],
)
def test_get_job_db(tmp_path, filename, expected):
    path = tmp_path / filename
    db = get_job_db(path)
    assert isinstance(db, expected)
    assert not path.exists()


@pytest.mark.parametrize(
    ["filename", "expected"],
    [
        ("jobz.csv", CsvJobDatabase),
        ("jobz.parquet", ParquetJobDatabase),
    ],
)
def test_create_job_db(tmp_path, filename, expected):
    df = pd.DataFrame({"year": [2023, 2024]})
    path = tmp_path / filename
    db = create_job_db(path=path, df=df)
    assert isinstance(db, expected)
    assert path.exists()


class TestProcessBasedJobCreator:
    @pytest.fixture
    def dummy_backend(self, requests_mock, con) -> DummyBackend:
        dummy = DummyBackend(requests_mock=requests_mock, connection=con)
        dummy.setup_simple_job_status_flow(queued=2, running=3, final="finished")
        return dummy

    PG_3PLUS5 = {
        "id": "3plus5",
        "process_graph": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
    }
    PG_INCREMENT = {
        "id": "increment",
        "parameters": [
            {"name": "data", "description": "data", "schema": {"type": "number"}},
            {
                "name": "increment",
                "description": "increment",
                "schema": {"type": "number"},
                "optional": True,
                "default": 1,
            },
        ],
        "process_graph": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "data"}, "y": {"from_parameter": "increment"}},
            "result": True,
        },
    }
    PG_OFFSET_POLYGON = {
        "id": "offset_polygon",
        "parameters": [
            {"name": "data", "description": "data", "schema": {"type": "number"}},
            {
                "name": "polygons",
                "description": "polygons",
                "schema": {
                    "title": "GeoJSON",
                    "type": "object",
                    "subtype": "geojson",
                },
            },
            {
                "name": "offset",
                "description": "Offset",
                "schema": {"type": "number"},
                "optional": True,
                "default": 0,
            },
        ],
    }

    @pytest.fixture(autouse=True)
    def remote_process_definitions(self, requests_mock) -> dict:
        mocks = {}
        processes = [self.PG_3PLUS5, self.PG_INCREMENT, self.PG_OFFSET_POLYGON]
        mocks["_all"] = requests_mock.get("https://remote.test/_all", json={"processes": processes, "links": []})
        for pg in processes:
            process_id = pg["id"]
            mocks[process_id] = requests_mock.get(f"https://remote.test/{process_id}.json", json=pg)
        return mocks

    def test_minimal(self, con, dummy_backend, remote_process_definitions):
        """Bare minimum: just start a job, no parameters/arguments"""
        job_factory = ProcessBasedJobCreator(process_id="3plus5", namespace="https://remote.test/3plus5.json")

        job = job_factory.start_job(row=pd.Series({"foo": 123}), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "3plus51": {
                        "process_id": "3plus5",
                        "namespace": "https://remote.test/3plus5.json",
                        "arguments": {},
                        "result": True,
                    }
                },
                "status": "created",
                "title": "Process '3plus5' with {}",
                "description": "Process '3plus5' (namespace https://remote.test/3plus5.json) with {}",
            }
        }

        assert remote_process_definitions["3plus5"].call_count == 1

    def test_basic(self, con, dummy_backend, remote_process_definitions):
        """Basic parameterized UDP job generation"""
        dummy_backend.extra_job_metadata_fields = ["title", "description"]
        job_factory = ProcessBasedJobCreator(process_id="increment", namespace="https://remote.test/increment.json")

        job = job_factory.start_job(row=pd.Series({"data": 123}), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 123, "increment": 1},
                        "result": True,
                    }
                },
                "status": "created",
                "title": "Process 'increment' with {'data': 123, 'increment': 1}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 123, 'increment': 1}",
            }
        }
        assert remote_process_definitions["increment"].call_count == 1

    @pytest.mark.parametrize(
        ["parameter_defaults", "row", "expected_arguments"],
        [
            (None, {"data": 123}, {"data": 123, "increment": 1}),
            (None, {"data": 123, "increment": 5}, {"data": 123, "increment": 5}),
            ({"increment": 5}, {"data": 123}, {"data": 123, "increment": 5}),
            ({"increment": 5}, {"data": 123, "increment": 1000}, {"data": 123, "increment": 1000}),
        ],
    )
    def test_basic_parameterization(self, con, dummy_backend, parameter_defaults, row, expected_arguments):
        """Basic parameterized UDP job generation"""
        job_factory = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_defaults=parameter_defaults,
        )

        job = job_factory.start_job(row=pd.Series(row), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments,
                        "result": True,
                    }
                },
                "status": "created",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment' .*"),
            }
        }

    @pytest.mark.parametrize(
        ["process_id", "namespace", "expected"],
        [
            (
                # Classic UDP reference
                "3plus5",
                None,
                {"process_id": "3plus5"},
            ),
            (
                # Remote process definition (with "redundant" process_id)
                "3plus5",
                "https://remote.test/3plus5.json",
                {"process_id": "3plus5", "namespace": "https://remote.test/3plus5.json"},
            ),
            (
                # Remote process definition with just namespace (process_id should be inferred from that)
                None,
                "https://remote.test/3plus5.json",
                {"process_id": "3plus5", "namespace": "https://remote.test/3plus5.json"},
            ),
            (
                # Remote process definition from listing
                "3plus5",
                "https://remote.test/_all",
                {"process_id": "3plus5", "namespace": "https://remote.test/_all"},
            ),
        ],
    )
    def test_process_references_in_constructor(
        self, con, requests_mock, dummy_backend, remote_process_definitions, process_id, namespace, expected
    ):
        """Various ways to provide process references in the constructor"""

        # Register personal UDP
        requests_mock.get(con.build_url("/process_graphs/3plus5"), json=self.PG_3PLUS5)

        job_factory = ProcessBasedJobCreator(process_id=process_id, namespace=namespace)

        job = job_factory.start_job(row=pd.Series({"foo": 123}), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {"3plus51": {**expected, "arguments": {}, "result": True}},
                "status": "created",
                "title": "Process '3plus5' with {}",
                "description": f"Process '3plus5' (namespace {namespace}) with {{}}",
            }
        }

    def test_no_process_id_nor_namespace(self):
        with pytest.raises(ValueError, match="At least one of `process_id` and `namespace` should be provided"):
            _ = ProcessBasedJobCreator()

    @pytest.fixture
    def job_manager(self, tmp_path, dummy_backend) -> MultiBackendJobManager:
        job_manager = MultiBackendJobManager(root_dir=tmp_path / "job_mgr_root")
        job_manager.add_backend("dummy", connection=dummy_backend.connection, parallel_jobs=1)
        return job_manager

    def test_with_job_manager_remote_basic(
        self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock, remote_process_definitions
    ):
        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_defaults={"increment": 5},
        )

        df = pd.DataFrame({"data": [1, 2, 3]})
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 3,
                "job start": 3,
                "job started running": 3,
                "job finished": 3,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        # Verify caching of HTTP request of remote process definition
        assert remote_process_definitions["increment"].call_count == 1

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 1, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 1, 'increment': 5}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 1, 'increment': 5}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 2, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 2, 'increment': 5}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 2, 'increment': 5}",
            },
            "job-002": {
                "job_id": "job-002",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 3, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 3, 'increment': 5}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 3, 'increment': 5}",
            },
        }

    @pytest.mark.parametrize(
        ["parameter_defaults", "df_data", "expected_arguments"],
        [
            (
                {"increment": 5},
                {"data": [1, 2, 3]},
                {
                    "job-000": {"data": 1, "increment": 5},
                    "job-001": {"data": 2, "increment": 5},
                    "job-002": {"data": 3, "increment": 5},
                },
            ),
            (
                None,
                {"data": [1, 2, 3], "increment": [44, 55, 66]},
                {
                    "job-000": {"data": 1, "increment": 44},
                    "job-001": {"data": 2, "increment": 55},
                    "job-002": {"data": 3, "increment": 66},
                },
            ),
            (
                {"increment": 5555},
                {"data": [1, 2, 3], "increment": [44, 55, 66]},
                {
                    "job-000": {"data": 1, "increment": 44},
                    "job-001": {"data": 2, "increment": 55},
                    "job-002": {"data": 3, "increment": 66},
                },
            ),
        ],
    )
    def test_with_job_manager_remote_parameter_handling(
        self,
        tmp_path,
        requests_mock,
        dummy_backend,
        job_manager,
        sleep_mock,
        parameter_defaults,
        df_data,
        expected_arguments,
    ):
        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_defaults=parameter_defaults,
        )

        df = pd.DataFrame(df_data)
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 3,
                "job start": 3,
                "job finished": 3,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments["job-000"],
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment'.*"),
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments["job-001"],
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment'.*"),
            },
            "job-002": {
                "job_id": "job-002",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments["job-002"],
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment'.*"),
            },
        }

    def test_with_job_manager_remote_geometry(self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock):
        job_starter = ProcessBasedJobCreator(
            process_id="offset_polygon",
            namespace="https://remote.test/offset_polygon.json",
            parameter_defaults={"data": 123},
        )

        df = geopandas.GeoDataFrame.from_features(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "one",
                        "properties": {"offset": 11},
                        "geometry": {"type": "Point", "coordinates": (1.0, 2.0)},
                    },
                    {
                        "type": "Feature",
                        "id": "two",
                        "properties": {"offset": 22},
                        "geometry": {"type": "Point", "coordinates": (3.0, 4.0)},
                    },
                ],
            }
        )

        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 2,
                "job start": 2,
                "job finished": 2,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [1.0, 2.0]},
                            "offset": 11,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'offset_polygon' with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (1...",
                "description": "Process 'offset_polygon' (namespace https://remote.test/offset_polygon.json) with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (1.0, 2.0)}, 'offset': 11}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [3.0, 4.0]},
                            "offset": 22,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'offset_polygon' with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (3...",
                "description": "Process 'offset_polygon' (namespace https://remote.test/offset_polygon.json) with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (3.0, 4.0)}, 'offset': 22}",
            },
        }

    @pytest.mark.parametrize(
        ["db_class"],
        [
            (CsvJobDatabase,),
            (ParquetJobDatabase,),
        ],
    )
    def test_with_job_manager_remote_geometry_after_resume(
        self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock, db_class
    ):
        """Test if geometry handling works properly after resuming from CSV serialized job db."""
        job_starter = ProcessBasedJobCreator(
            process_id="offset_polygon",
            namespace="https://remote.test/offset_polygon.json",
            parameter_defaults={"data": 123},
        )

        df = geopandas.GeoDataFrame.from_features(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "one",
                        "properties": {"offset": 11},
                        "geometry": {"type": "Point", "coordinates": (1.0, 2.0)},
                    },
                    {
                        "type": "Feature",
                        "id": "two",
                        "properties": {"offset": 22},
                        "geometry": {"type": "Point", "coordinates": (3.0, 4.0)},
                    },
                ],
            }
        )

        # Persist the job db to CSV/Parquet/...
        job_db_path = tmp_path / "jobs.db"
        _ = db_class(job_db_path).initialize_from_df(df)
        assert job_db_path.exists()

        # Resume from persisted job db
        job_db = db_class(job_db_path)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 2,
                "job start": 2,
                "job finished": 2,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [1.0, 2.0]},
                            "offset": 11,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'offset_polygon' with.*"),
                "description": dirty_equals.IsStr(regex="Process 'offset_polygon' .*"),
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [3.0, 4.0]},
                            "offset": 22,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'offset_polygon' with.*"),
                "description": dirty_equals.IsStr(regex="Process 'offset_polygon' .*"),
            },
        }

    def test_with_job_manager_udp_basic(
        self, tmp_path, requests_mock, con, dummy_backend, job_manager, sleep_mock, remote_process_definitions
    ):
        # make deep copy
        udp = copy.deepcopy(self.PG_INCREMENT)
        # Register personal UDP
        increment_udp_mock = requests_mock.get(con.build_url("/process_graphs/increment"), json=udp)

        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            # No namespace to trigger personal UDP mode
            namespace=None,
            parameter_defaults={"increment": 5},
        )
        assert increment_udp_mock.call_count == 0

        df = pd.DataFrame({"data": [3, 5]})
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 2,
                "job finished": 2,
            }
        )
        assert increment_udp_mock.call_count == 2
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "arguments": {"data": 3, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 3, 'increment': 5}",
                "description": "Process 'increment' (namespace None) with {'data': 3, 'increment': 5}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "arguments": {"data": 5, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 5, 'increment': 5}",
                "description": "Process 'increment' (namespace None) with {'data': 5, 'increment': 5}",
            },
        }

    def test_with_job_manager_parameter_column_map(
        self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock, remote_process_definitions
    ):
        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_column_map={"data": "numberzzz", "increment": "add_thiz"},
        )

        df = pd.DataFrame(
            {
                "data": [1, 2],
                "increment": [-1, -2],
                "numberzzz": [3, 5],
                "add_thiz": [100, 200],
            }
        )
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 2,
                "job finished": 2,
            }
        )
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 3, "increment": 100},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 3, 'increment': 100}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 3, 'increment': 100}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 5, "increment": 200},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 5, 'increment': 200}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 5, 'increment': 200}",
            },
        }

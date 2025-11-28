import collections
import dataclasses
import datetime
import json
import logging
import re
import threading
from pathlib import Path
from time import sleep
from typing import Union
from unittest import mock

import dirty_equals

# TODO: can we avoid using httpretty?
#   We need it for testing the resilience, which uses an HTTPadapter with Retry
#   but requests-mock also uses an HTTPAdapter for the mocking and basically
#   erases the HTTPAdapter we have set up.
#   httpretty avoids this specific problem because it mocks at the socket level,
#   But I would rather not have two dependencies with almost the same goal.
import httpretty
import numpy as np
import pandas as pd
import pytest
import requests

import openeo
from openeo import BatchJob
from openeo.extra.job_management._job_db import (
    CsvJobDatabase,
    ParquetJobDatabase,
    create_job_db,
)
from openeo.extra.job_management._manager import MAX_RETRIES, MultiBackendJobManager
from openeo.extra.job_management._thread_worker import (
    Task,
    _JobManagerWorkerThreadPool,
    _TaskResult,
)
from openeo.rest._testing import DummyBackend
from openeo.rest.auth.testing import OidcMock
from openeo.util import load_json, rfc3339


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


@dataclasses.dataclass(frozen=True)
class DummyResultTask(Task):
    """
    A dummy task to directly define a _TaskResult.
    """

    db_update: dict = dataclasses.field(default_factory=dict)
    stats_update: dict = dataclasses.field(default_factory=dict)

    def execute(self) -> _TaskResult:
        return _TaskResult(
            job_id=self.job_id,
            df_idx=self.df_idx,
            db_update=self.db_update,
            stats_update=self.stats_update,
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

    def test_start_job_thread_basic(self, tmp_path, job_manager, job_manager_root_dir, sleep_mock):
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
        # TODO: better way than sleeping to make sure the job thread does all its work?
        sleep(2)
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
        df_normalized = MultiBackendJobManager._column_requirements.normalize_df(df)
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
        with mock.patch("time.sleep", new=lambda s: time_machine.shift(60 * 60)):
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
                None,  # Missing running_start_time
                "2024-09-01T20:00:00Z",  # Job end time
                "finished",  # Job final status
                6 * 60 * 60,  # Cancel after 6 hours
            ),
            # Scenario 2: NaN running_start_time
            (
                "2024-09-01T09:00:00Z",
                "2024-09-01T09:00:00Z",
                float("nan"),  # NaN running_start_time
                "2024-09-01T20:00:00Z",  # Job end time
                "finished",  # Job final status
                6 * 60 * 60,  # Cancel after 6 hours
            ),
        ],
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
        df = pd.DataFrame(
            {
                "year": [2024],
                "running_start_time": [running_start_time],  # Initial running_start_time
            }
        )

        # Move the time machine to the job creation time
        time_machine.move_to(create_time)

        job_db_path = tmp_path / "jobs.csv"

        # Mock sleep() to skip one hour at a time instead of actually sleeping
        with mock.patch("time.sleep", new=lambda s: time_machine.shift(60 * 60)):
            job_manager.run_jobs(df=df, start_job=self._create_year_job, job_db=job_db_path)

        final_df = CsvJobDatabase(job_db_path).read()

        # Validate running_start_time is a valid datetime object
        filled_running_start_time = final_df.iloc[0]["running_start_time"]
        assert isinstance(rfc3339.parse_datetime(filled_running_start_time), datetime.datetime)

    def test_process_threadworker_updates(self, tmp_path, caplog):
        pool = _JobManagerWorkerThreadPool(max_workers=2)
        stats = collections.defaultdict(int)

        # Submit tasks covering all cases
        pool.submit_task(DummyResultTask("j-0", df_idx=0, db_update={"status": "queued"}, stats_update={"queued": 1}))
        pool.submit_task(DummyResultTask("j-1", df_idx=1, db_update={"status": "queued"}, stats_update={}))
        pool.submit_task(DummyResultTask("j-2", df_idx=2, db_update={}, stats_update={"queued": 1}))
        pool.submit_task(DummyResultTask("j-3", df_idx=3, db_update={}, stats_update={}))

        df_initial = pd.DataFrame(
            {
                "id": ["j-0", "j-1", "j-2", "j-3"],
                "status": ["created", "created", "created", "created"],
            }
        )
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df_initial)

        mgr = MultiBackendJobManager(root_dir=tmp_path / "jobs")

        mgr._process_threadworker_updates(worker_pool=pool, job_db=job_db, stats=stats)

        df_final = job_db.read()
        pd.testing.assert_frame_equal(
            df_final[["id", "status"]],
            pd.DataFrame(
                {
                    "id": ["j-0", "j-1", "j-2", "j-3"],
                    "status": ["queued", "queued", "created", "created"],
                }
            ),
        )
        assert stats == dirty_equals.IsPartialDict(
            {
                "queued": 2,
                "job_db persist": 1,
            }
        )
        assert caplog.messages == []

    def test_process_threadworker_updates_unknown(self, tmp_path, caplog):
        pool = _JobManagerWorkerThreadPool(max_workers=2)
        stats = collections.defaultdict(int)

        pool.submit_task(DummyResultTask("j-123", df_idx=0, db_update={"status": "queued"}, stats_update={"queued": 1}))
        pool.submit_task(DummyResultTask("j-unknown", df_idx=4, db_update={"status": "created"}, stats_update={}))

        df_initial = pd.DataFrame(
            {
                "id": ["j-123", "j-456"],
                "status": ["created", "created"],
            }
        )
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df_initial)

        mgr = MultiBackendJobManager(root_dir=tmp_path / "jobs")

        mgr._process_threadworker_updates(worker_pool=pool, job_db=job_db, stats=stats)

        df_final = job_db.read()
        pd.testing.assert_frame_equal(
            df_final[["id", "status"]],
            pd.DataFrame(
                {
                    "id": ["j-123", "j-456"],
                    "status": ["queued", "created"],
                }
            ),
        )
        assert stats == dirty_equals.IsPartialDict(
            {
                "queued": 1,
                "job_db persist": 1,
            }
        )
        assert caplog.messages == [dirty_equals.IsStr(regex=".*Ignoring unknown.*indices.*4.*")]

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

    def test_refresh_bearer_token_before_start(
        self,
        tmp_path,
        job_manager,
        dummy_backend_foo,
        dummy_backend_bar,
        job_manager_root_dir,
        sleep_mock,
        requests_mock,
    ):
        client_id = "client123"
        client_secret = "$3cr3t"
        oidc_issuer = "https://oidc.test/"
        oidc_mock = OidcMock(
            requests_mock=requests_mock,
            expected_grant_type="client_credentials",
            expected_client_id=client_id,
            expected_fields={"client_secret": client_secret, "scope": "openid"},
            oidc_issuer=oidc_issuer,
        )
        dummy_backend_foo.setup_credentials_oidc(issuer=oidc_issuer)
        dummy_backend_bar.setup_credentials_oidc(issuer=oidc_issuer)
        dummy_backend_foo.connection.authenticate_oidc_client_credentials(client_id="client123", client_secret="$3cr3t")
        dummy_backend_bar.connection.authenticate_oidc_client_credentials(client_id="client123", client_secret="$3cr3t")

        # After this setup, we have 2 client credential token requests (one for each backend)
        assert len(oidc_mock.grant_request_history) == 2

        df = pd.DataFrame({"year": [2020, 2021, 2022, 2023, 2024]})
        job_db_path = tmp_path / "jobs.csv"
        job_db = CsvJobDatabase(job_db_path).initialize_from_df(df)
        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)

        assert run_stats == dirty_equals.IsPartialDict(
            {
                "job_queued_for_start": 5,
                "job started running": 5,
                "job finished": 5,
            }
        )

        # Because of proactive+throttled token refreshing,
        # we should have 2 additional token requests now
        assert len(oidc_mock.grant_request_history) == 4

    @pytest.mark.parametrize(
        ["download_results"],
        [
            (True,),
            (False,),
        ],
    )
    def test_download_results_toggle(
        self, tmp_path, job_manager_root_dir, dummy_backend_foo, download_results, sleep_mock
    ):
        job_manager = MultiBackendJobManager(root_dir=job_manager_root_dir, download_results=download_results)
        job_manager.add_backend("foo", connection=dummy_backend_foo.connection)

        df = pd.DataFrame({"year": [2018, 2019]})
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)
        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)
        assert run_stats == dirty_equals.IsPartialDict({"job finished": 2})

        if download_results:
            assert (job_manager_root_dir / "job_job-2018/result.data").read_bytes() == DummyBackend.DEFAULT_RESULT
            assert load_json(job_manager_root_dir / "job_job-2018/job_job-2018.json") == dirty_equals.IsPartialDict(
                id="job-2018", status="finished"
            )
            assert (job_manager_root_dir / "job_job-2019/result.data").read_bytes() == DummyBackend.DEFAULT_RESULT
            assert load_json(job_manager_root_dir / "job_job-2019/job_job-2019.json") == dirty_equals.IsPartialDict(
                id="job-2019", status="finished"
            )
        else:
            assert not job_manager_root_dir.exists() or list(job_manager_root_dir.iterdir()) == []

import copy
import json
import re
import threading
from time import sleep
from typing import Callable, Union
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
from openeo.rest._testing import OPENEO_BACKEND, DummyBackend, build_capabilities
from openeo.util import rfc3339


@pytest.fixture
def con(requests_mock) -> openeo.Connection:
    requests_mock.get(OPENEO_BACKEND, json=build_capabilities(api_version="1.2.0", udp=True))
    con = openeo.Connection(OPENEO_BACKEND)
    return con


class FakeBackend:
    """
    Fake openEO backend with some basic job management functionality for testing job manager logic.
    """

    # TODO: replace/merge with openeo.rest._testing.DummyBackend

    def __init__(self, *, backend_root_url: str = "http://openeo.test", requests_mock):
        self.url = backend_root_url.rstrip("/")
        requests_mock.get(f"{self.url}/", json={"api_version": "1.1.0"})
        self.job_db = {}
        self.get_job_metadata_mock = requests_mock.get(
            re.compile(rf"^{self.url}/jobs/[\w_-]*$"),
            json=self._handle_get_job_metadata,
        )
        self.cancel_job_mock = requests_mock.delete(
            re.compile(rf"^{self.url}/jobs/[\w_-]*/results$"),
            json=self._handle_cancel_job,
        )
        requests_mock.get(re.compile(rf"^{self.url}/jobs/[\w_-]*/results"), json={"links": []})

    def set_job_status(self, job_id: str, status: Union[str, Callable[[], str]]):
        self.job_db.setdefault(job_id, {})["status"] = status

    def get_job_status(self, job_id: str):
        status = self.job_db[job_id]["status"]
        if callable(status):
            status = status()
        return status

    def _handle_get_job_metadata(self, request, context):
        job_id = request.path.split("/")[-1]
        return {"id": job_id, "status": self.get_job_status(job_id)}

    def _handle_cancel_job(self, request, context):
        job_id = request.path.split("/")[-2]
        assert self.get_job_status(job_id) == "running"
        self.set_job_status(job_id, "canceled")
        context.status_code = 204


@pytest.fixture
def sleep_mock():
    with mock.patch("time.sleep") as sleep:
        yield sleep

class TestMultiBackendJobManager:



    def test_basic_legacy(self, tmp_path, requests_mock, sleep_mock):
        """
        Legacy `run_jobs()` usage with explicit dataframe and output file
        """
        manager = self._create_basic_mocked_manager(requests_mock, tmp_path)

        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        output_file = tmp_path / "jobs.csv"

        def start_job(row, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        run_stats = manager.run_jobs(df=df, start_job=start_job, output_file=output_file)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=10),
                "start_job call": 7,  # TODO?
                "job started running": 5,
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
                "run_jobs loop": dirty_equals.IsInt(gt=5),
            }
        )

        result = pd.read_csv(output_file)
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

        # We expect that the job metadata was saved, so verify that it exists.
        # Checking for one of the jobs is enough.
        metadata_path = manager.get_job_metadata_path(job_id="job-2022")
        assert metadata_path.exists()

    def test_basic(self, tmp_path, requests_mock, sleep_mock):
        """
        `run_jobs()` usage with a `CsvJobDatabase`
        (and no explicit dataframe or output file)
        """
        manager = self._create_basic_mocked_manager(requests_mock, tmp_path)

        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        output_file = tmp_path / "jobs.csv"

        def start_job(row, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        job_db = CsvJobDatabase(output_file).initialize_from_df(df)

        run_stats = manager.run_jobs(job_db=job_db, start_job=start_job)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=10),
                "start_job call": 7,  # TODO?
                "job started running": 5,
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
                "run_jobs loop": dirty_equals.IsInt(gt=5),
            }
        )

        result = pd.read_csv(output_file)
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

        # We expect that the job metadata was saved, so verify that it exists.
        # Checking for one of the jobs is enough.
        metadata_path = manager.get_job_metadata_path(job_id="job-2022")
        assert metadata_path.exists()

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_db_class(self, tmp_path, requests_mock, sleep_mock, db_class):
        """
        Basic run parameterized on database class
        """
        manager = self._create_basic_mocked_manager(requests_mock, tmp_path)

        def start_job(row, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        df = pd.DataFrame({"year": [2018, 2019, 2020, 2021, 2022]})
        output_file = tmp_path / "jobs.db"
        job_db = db_class(output_file).initialize_from_df(df)

        run_stats = manager.run_jobs(job_db=job_db, start_job=start_job)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 7,  # TODO?
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
            }
        )

        result = job_db.read()
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

    @pytest.mark.parametrize(
        ["filename", "expected_db_class"],
        [
            ("jobz.csv", CsvJobDatabase),
            ("jobz.parquet", ParquetJobDatabase),
        ],
    )
    def test_create_job_db(self, tmp_path, requests_mock, sleep_mock, filename, expected_db_class):
        """
        Basic run with `create_job_db()` usage
        """
        manager = self._create_basic_mocked_manager(requests_mock, tmp_path)

        def start_job(row, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        df = pd.DataFrame({"year": [2018, 2019, 2020, 2021, 2022]})
        output_file = tmp_path / filename
        job_db = create_job_db(path=output_file, df=df)

        run_stats = manager.run_jobs(job_db=job_db, start_job=start_job)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 7,  # TODO?
                "job finished": 5,
                "job_db persist": dirty_equals.IsInt(gt=5),
            }
        )

        result = job_db.read()
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

    def test_basic_threading(self, tmp_path, requests_mock, sleep_mock):
        manager = self._create_basic_mocked_manager(requests_mock, tmp_path)

        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        output_file = tmp_path / "jobs.csv"

        def start_job(row, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        job_db = CsvJobDatabase(output_file).initialize_from_df(df)

        manager.start_job_thread(start_job=start_job, job_db=job_db)
        # Trigger context switch to job thread
        sleep(1)
        manager.stop_job_thread()
        # TODO #645 how to collect stats with the threaded run_job?
        assert sleep_mock.call_count > 10

        result = pd.read_csv(output_file)
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

        # We expect that the job metadata was saved, so verify that it exists.
        # Checking for one of the jobs is enough.
        metadata_path = manager.get_job_metadata_path(job_id="job-2022")
        assert metadata_path.exists()

    def _create_basic_mocked_manager(self, requests_mock, tmp_path):
        # TODO: separate aspects of job manager and dummy backends
        requests_mock.get("http://foo.test/", json={"api_version": "1.1.0"})
        requests_mock.get("http://bar.test/", json={"api_version": "1.1.0"})

        def mock_job_status(job_id, queued=1, running=2):
            """Mock job status polling sequence"""
            response_list = sum(
                [
                    [
                        {
                            "json": {
                                "id": job_id,
                                "title": f"Job {job_id}",
                                "status": "queued",
                            }
                        }
                    ]
                    * queued,
                    [
                        {
                            "json": {
                                "id": job_id,
                                "title": f"Job {job_id}",
                                "status": "running",
                            }
                        }
                    ]
                    * running,
                    [
                        {
                            "json": {
                                "id": job_id,
                                "title": f"Job {job_id}",
                                "status": "finished",
                            }
                        }
                    ],
                ],
                [],
            )
            for backend in ["http://foo.test", "http://bar.test"]:
                requests_mock.get(f"{backend}/jobs/{job_id}", response_list)
                # It also needs the job results endpoint, though that can be a dummy implementation.
                # When the job is finished the system tries to download the results and that is what
                # needs this endpoint.
                requests_mock.get(f"{backend}/jobs/{job_id}/results", json={"links": []})

        mock_job_status("job-2018", queued=1, running=2)
        mock_job_status("job-2019", queued=2, running=3)
        mock_job_status("job-2020", queued=3, running=4)
        mock_job_status("job-2021", queued=3, running=5)
        mock_job_status("job-2022", queued=5, running=6)
        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(root_dir=root_dir)
        manager.add_backend("foo", connection=openeo.connect("http://foo.test"))
        manager.add_backend("bar", connection=openeo.connect("http://bar.test"))
        return manager

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
            ]
        )

    def test_manager_must_exit_when_all_jobs_done(self, tmp_path, requests_mock, sleep_mock):
        """Make sure the MultiBackendJobManager does not hang after all processes finish.

        Regression test for:
        https://github.com/Open-EO/openeo-python-client/issues/432

        Cause was that the run_jobs had an infinite loop when jobs ended with status error.
        """

        requests_mock.get("http://foo.test/", json={"api_version": "1.1.0"})
        requests_mock.get("http://bar.test/", json={"api_version": "1.1.0"})

        def mock_job_status(job_id, succeeds: bool):
            """Mock job status polling sequence.
            We set up one job with finishes with status error
            """
            response_list = sum(
                [
                    [
                        {
                            "json": {
                                "id": job_id,
                                "title": f"Job {job_id}",
                                "status": "queued",
                            }
                        }
                    ],
                    [
                        {
                            "json": {
                                "id": job_id,
                                "title": f"Job {job_id}",
                                "status": "running",
                            }
                        }
                    ],
                    [
                        {
                            "json": {
                                "id": job_id,
                                "title": f"Job {job_id}",
                                "status": "finished" if succeeds else "error",
                            }
                        }
                    ],
                ],
                [],
            )
            for backend in ["http://foo.test", "http://bar.test"]:
                requests_mock.get(f"{backend}/jobs/{job_id}", response_list)
                # It also needs job results endpoint for succesful jobs and the
                # log endpoint for a failed job. Both are dummy implementations.
                # When the job is finished the system tries to download the
                # results or the logs and that is what needs these endpoints.
                if succeeds:
                    requests_mock.get(f"{backend}/jobs/{job_id}/results", json={"links": []})
                else:
                    response = {
                        "level": "error",
                        "logs": [
                            {
                                "id": "1",
                                "code": "SampleError",
                                "level": "error",
                                "message": "Error for testing",
                                "time": "2019-08-24T14:15:22Z",
                                "data": None,
                                "path": [],
                                "usage": {},
                                "links": [],
                            }
                        ],
                        "links": [],
                    }
                    requests_mock.get(f"{backend}/jobs/{job_id}/logs?level=error", json=response)

        mock_job_status("job-2018", succeeds=True)
        mock_job_status("job-2019", succeeds=True)
        mock_job_status("job-2020", succeeds=True)
        mock_job_status("job-2021", succeeds=True)
        mock_job_status("job-2022", succeeds=False)

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(root_dir=root_dir)

        manager.add_backend("foo", connection=openeo.connect("http://foo.test"))
        manager.add_backend("bar", connection=openeo.connect("http://bar.test"))

        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        output_file = tmp_path / "jobs.csv"

        def start_job(row, connection, **kwargs):
            year = int(row["year"])
            return BatchJob(job_id=f"job-{year}", connection=connection)

        is_done_file = tmp_path / "is_done.txt"

        def start_worker_thread():
            manager.run_jobs(df=df, start_job=start_job, output_file=output_file)
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

        # Also check that we got sensible end results.
        result = pd.read_csv(output_file)
        assert len(result) == 5
        assert set(result.status) == {"finished", "error"}
        assert set(result.backend_name) == {"foo", "bar"}

        # We expect that the job metadata was saved for a successful job,
        # so verify that it exists.
        # Checking it for one of the jobs is enough.
        metadata_path = manager.get_job_metadata_path(job_id="job-2021")
        assert metadata_path.exists()

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

        output_file = tmp_path / "jobs.csv"

        run_stats = manager.run_jobs(df=df, start_job=start_job, output_file=output_file)
        assert run_stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 1,
            }
        )

        # Sanity check: the job succeeded
        result = pd.read_csv(output_file)
        assert len(result) == 1
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo"}

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

        output_file = tmp_path / "jobs.csv"

        with pytest.raises(requests.exceptions.RetryError) as exc:
            manager.run_jobs(df=df, start_job=start_job, output_file=output_file)

        # TODO #645 how to still check stats when run_jobs raised exception?
        assert sleep_mock.call_count > 3

        # Sanity check: the job has status "error"
        result = pd.read_csv(output_file)
        assert len(result) == 1
        assert set(result.status) == {"running"}
        assert set(result.backend_name) == {"foo"}

    @pytest.mark.parametrize(
        ["start_time", "end_time", "end_status", "cancel_after_seconds", "expected_status"],
        [
            ("2024-09-01T10:00:00Z", "2024-09-01T20:00:00Z", "finished", 6 * 60 * 60, "canceled"),
            ("2024-09-01T10:00:00Z", "2024-09-01T20:00:00Z", "finished", 12 * 60 * 60, "finished"),
        ],
    )
    def test_automatic_cancel_of_too_long_running_jobs(
        self,
        requests_mock,
        tmp_path,
        time_machine,
        start_time,
        end_time,
        end_status,
        cancel_after_seconds,
        expected_status,
    ):
        fake_backend = FakeBackend(requests_mock=requests_mock)

        # For simplicity, set up pre-existing job with status "running" (instead of job manager creating+starting it)
        job_id = "job-123"
        fake_backend.set_job_status(job_id, lambda: "running" if rfc3339.utcnow() < end_time else end_status)

        manager = MultiBackendJobManager(root_dir=tmp_path, cancel_running_job_after=cancel_after_seconds)
        manager.add_backend("foo", connection=openeo.connect(fake_backend.url))

        # Initialize data frame with status "created" (to make sure the start of "running" state is recorded)
        df = pd.DataFrame({"id": [job_id], "backend_name": ["foo"], "status": ["created"]})

        time_machine.move_to(start_time)
        # Mock sleep() to not actually sleep, but skip one hour at a time
        with mock.patch.object(openeo.extra.job_management.time, "sleep", new=lambda s: time_machine.shift(60 * 60)):
            manager.run_jobs(df=df, start_job=lambda **kwargs: None, job_db=tmp_path / "jobs.csv")

        final_df = CsvJobDatabase(tmp_path / "jobs.csv").read()
        assert final_df.iloc[0].to_dict() == dirty_equals.IsPartialDict(
            id="job-123", status=expected_status, running_start_time="2024-09-01T10:00:00Z"
        )

        assert fake_backend.cancel_job_mock.called == (expected_status == "canceled")




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
        path = tmp_path / "jobs.csv"

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
        assert db.count_by_status(statuses=["not_started"])["not_started"] >1

        assert len(loaded) == 2
        loaded.loc[0,"status"] = "running"
        loaded.loc[1, "status"] = "error"
        db.persist(loaded)
        assert db.count_by_status(statuses=["error"])["error"] == 1

        all = db.read()
        assert len(all) == len(orig)
        assert all.loc[0,"status"] == "running"
        assert all.loc[1,"status"] == "error"
        if(len(all) >2):
            assert all.loc[2,"status"] == "not_started"
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

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
)
from openeo.util import rfc3339


class FakeBackend:
    """
    Fake openEO backend with some basic job management functionality for testing job manager logic.
    """
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


class TestMultiBackendJobManager:


    @pytest.fixture
    def sleep_mock(self):
        with mock.patch("time.sleep") as sleep:
            yield sleep

    def test_basic(self, tmp_path, requests_mock, sleep_mock):
        manager = self.create_basic_mocked_manager(requests_mock, tmp_path)

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

        manager.run_jobs(df=df, start_job=start_job, output_file=output_file)
        assert sleep_mock.call_count > 10

        result = pd.read_csv(output_file)
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

        # We expect that the job metadata was saved, so verify that it exists.
        # Checking for one of the jobs is enough.
        metadata_path = manager.get_job_metadata_path(job_id="job-2022")
        assert metadata_path.exists()

    def test_basic_threading(self, tmp_path, requests_mock, sleep_mock):
        manager = self.create_basic_mocked_manager(requests_mock, tmp_path)

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

        job_db = CsvJobDatabase(output_file)
        # TODO: avoid private _normalize_df API
        job_db.persist(manager._normalize_df(df))

        manager.start_job_thread(start_job=start_job, job_db=job_db)
        # Trigger context switch to job thread
        sleep(1)
        manager.stop_job_thread()
        assert sleep_mock.call_count > 10

        result = pd.read_csv(output_file)
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

        # We expect that the job metadata was saved, so verify that it exists.
        # Checking for one of the jobs is enough.
        metadata_path = manager.get_job_metadata_path(job_id="job-2022")
        assert metadata_path.exists()

    def create_basic_mocked_manager(self, requests_mock, tmp_path):
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
        df = pd.DataFrame(
            {
                "some_number": [3, 2, 1],
            }
        )

        df_normalized = MultiBackendJobManager()._normalize_df(df)

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

        manager.run_jobs(df=df, start_job=start_job, output_file=output_file)
        assert sleep_mock.call_count > 3

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
        if not hasattr(time_machine, "shift"):
            # TODO #578 remove this hack to skip this test on Python 3.7
            #       `time_machine.shift` is only available since timemachine>=2.13.0, which only support Python 3.8 and up
            pytest.skip("time_machine.shift not available")

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
        path = tmp_path / "jobs.parquet"

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

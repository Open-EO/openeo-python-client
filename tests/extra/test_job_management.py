import json
import threading
from unittest import mock
import datetime
import time_machine
from openeo.util import rfc3339


# TODO: can we avoid using httpretty?
#   We need it for testing the resilience, which uses an HTTPadapter with Retry
#   but requests-mock also uses an HTTPAdapter for the mocking and basically
#   erases the HTTPAdapter we have set up.
#   httpretty avoids this specific problem because it mocks at the socket level,
#   But I would rather not have two dependencies with almost the same goal.
import httpretty
import pandas as pd
import pytest
import requests
import shapely.geometry.point as shpt

import openeo
from openeo import BatchJob
from openeo.extra.job_management import (
    MAX_RETRIES,
    MultiBackendJobManager,
    _CsvJobDatabase,
    _ParquetJobDatabase,
)
from openeo.rest import OpenEoApiError




class TestMultiBackendJobManager:


    @pytest.fixture
    def sleep_mock(self):
        with mock.patch("time.sleep") as sleep:
            yield sleep
    

    def test_basic(self, tmp_path, requests_mock, sleep_mock):
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

        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020, 2021, 2022],
                # Use simple points in WKT format to test conversion to the geometry dtype
                "geometry": ["POINT (1 2)"] * 5,
            }
        )
        output_file = tmp_path / "jobs.csv"

        def start_job(row, connection, **kwargs):
            year = row["year"]
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
            year = row["year"]
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
            year = row["year"]
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
            year = row["year"]
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

    def test_cancel_prolonged_job_exceeds_duration(self):
        # Create mock BatchJob instance
        job = mock.MagicMock()  # Use MagicMock directly here
        job.job_id = "test_job_id"
        
        row = {
            "running_start_time": "2020-01-01T00:00:00Z"
        }
        
        # Initialize manager with the max_running_duration as seconds
        max_running_duration_seconds = 12 * 60 * 60  # 12 hours
        manager = MultiBackendJobManager(cancel_running_job_after=max_running_duration_seconds)
        
        # set up timestamps
        job_running_timestamp = rfc3339.parse_datetime(row["running_start_time"], with_timezone=True)
        future_time = job_running_timestamp + datetime.timedelta(seconds=max_running_duration_seconds) + datetime.timedelta(seconds=1)

        with time_machine.travel(future_time, tick=False):
            manager._cancel_prolonged_job(job, row)
                
        # Verify that the stop method was called
        job.stop.assert_called_once()
      


    def test_cancel_prolonged_job_within_duration(self):
        # Create mock BatchJob instance
        job = mock.MagicMock()
        job.job_id = "test_job_id"
        
        row = {
            "running_start_time": "2020-01-01T00:00:00Z"
        }
        
        # Initialize manager with the max_running_duration as seconds
        max_running_duration_seconds = 12 * 60 * 60  # 12 hours
        manager = MultiBackendJobManager(cancel_running_job_after=max_running_duration_seconds)
        
        # set up timestamps
        job_running_timestamp = rfc3339.parse_datetime(row["running_start_time"], with_timezone=True)
        future_time = job_running_timestamp + datetime.timedelta(seconds=max_running_duration_seconds) - datetime.timedelta(seconds=1)
        
        with time_machine.travel(future_time, tick=False):
            manager._cancel_prolonged_job(job, row)
            
        # Verify that the stop method was not called
        job.stop.assert_not_called()
       
    
class TestCsvJobDatabase:
    def test_read_wkt(self, tmp_path):
        wkt_df = pd.DataFrame(
            {
                "value": ["wkt"],
                "geometry": ["POINT (30 10)"],
            }
        )
        path = tmp_path / "jobs.csv"
        wkt_df.to_csv(path, index=False)
        df = _CsvJobDatabase(path).read()
        assert isinstance(df.geometry[0], shpt.Point)

    def test_read_non_wkt(self, tmp_path):
        non_wkt_df = pd.DataFrame(
            {
                "value": ["non_wkt"],
                "geometry": ["this is no WKT"],
            }
        )
        path = tmp_path / "jobs.csv"
        non_wkt_df.to_csv(path, index=False)
        df = _CsvJobDatabase(path).read()
        assert isinstance(df.geometry[0], str)

    def test_persist(self, tmp_path):
        df = pd.DataFrame(
            {
                "some_number": [3, 2, 1],
            }
        )

        path = tmp_path / "jobs.csv"
        _CsvJobDatabase(path).persist(df)
        assert _CsvJobDatabase(path).read().equals(df)


class TestParquetJobDatabase:
    def test_read_persist(self, tmp_path):
        df = pd.DataFrame(
            {
                "some_number": [3, 2, 1],
            }
        )

        path = tmp_path / "jobs.parquet"
        _ParquetJobDatabase(path).persist(df)
        assert _ParquetJobDatabase(path).read().equals(df)


#%%


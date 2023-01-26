import json

# TODO: can we avoid using httpretty?
#   We need it for testing the resilience, which uses an HTTPadapter with Retry
#   but requests-mock also uses an HTTPAdapter for the mocking and basically
#   erases the HTTPAdapter we have set up.
#   httpretty avoids this specific problem because it mocks at the socket level,
#   But I would rather not have two dependencies with almost the same goal.
import httpretty

# Alias to avoid conflicting names in httpretty.
from httpretty.core import httpretty as corehttpretty

import pandas as pd
import pytest
import requests
import shapely.geometry.point as shpt


import openeo
from openeo.extra.job_management import MultiBackendJobManager
from openeo import BatchJob

class TestMultiBackendJobManager:
    def test_basic(self, tmp_path, requests_mock):
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
                requests_mock.get(
                    f"{backend}/jobs/{job_id}/results", json={"links": []}
                )

        mock_job_status("job-2018", queued=1, running=2)
        mock_job_status("job-2019", queued=2, running=3)
        mock_job_status("job-2020", queued=3, running=4)
        mock_job_status("job-2021", queued=3, running=5)
        mock_job_status("job-2022", queued=5, running=6)

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(poll_sleep=0.2, root_dir=root_dir)

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

        result = pd.read_csv(output_file)
        assert len(result) == 5
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo", "bar"}

        # We expect that the job metadata was saved, so verify that it exists.
        # Checking for one of the jobs is enough.
        metadata_path = manager.get_job_metadata_path(job_id="job-2022")
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
        requests_mock.get(
            f"{backend}/jobs/{job_id}/logs", json={"logs": errors_log_lines}
        )

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(poll_sleep=0.2, root_dir=root_dir)
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

    def test_normalize_df_adds_required_columns(self):
        df = pd.DataFrame(
            {
                "some_number": [3, 2, 1],
            }
        )

        manager = MultiBackendJobManager()
        df_normalized = manager._normalize_df(df)

        assert set(df_normalized.columns) == set(
            [
                "some_number",
                "status",
                "id",
                "start_time",
                "cpu",
                "memory",
                "duration",
                "backend_name",
            ]
        )

    def test_normalize_df_converts_wkt_geometry_column(self):
        df = pd.DataFrame(
            {
                "some_number": [3, 2],
                "geometry": [
                    "Point (100 200)",
                    "Point (99 123)",
                    # "MULTIPOINT(0 0,1 1)",
                    # "LINESTRING(1.5 2.45,3.21 4)"
                ],
            }
        )

        manager = MultiBackendJobManager()
        df_normalized = manager._normalize_df(df)

        first_point = df_normalized.loc[0, "geometry"]
        second_point = df_normalized.loc[1, "geometry"]

        # The geometry columns should be converted so now it should contain
        # Point objects from the module shapely.geometry.point
        assert isinstance(first_point, shpt.Point)

        assert first_point == shpt.Point(100, 200)
        assert second_point == shpt.Point(99, 123)

    @httpretty.activate(allow_net_connect=False, verbose=True)
    @pytest.mark.parametrize("http_error_status", [502, 503, 504])
    def test_is_resilient_to_backend_failures(self, tmp_path, http_error_status):
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
        max_retries = 5

        httpretty.register_uri(
            "GET", backend, body=json.dumps({"api_version": "1.1.0"})
        )

        # First fail the max times the connection should retry, then succeed. after that
        response_list = [
            corehttpretty.Response(
                f"Simulate error HTTP {http_error_status}", status=http_error_status
            )
        ] * max_retries
        response_list += [
            corehttpretty.Response(
                body=json.dumps(
                    {
                        "id": job_id,
                        "title": f"Job {job_id}",
                        "status": "finished",
                    }
                )
            )
        ]
        httpretty.register_uri(
            "GET", f"{backend}/jobs/{job_id}", responses=response_list
        )

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(poll_sleep=0.2, root_dir=root_dir)
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

        # Sanity check: the job succeeded
        result = pd.read_csv(output_file)
        assert len(result) == 1
        assert set(result.status) == {"finished"}
        assert set(result.backend_name) == {"foo"}

    @httpretty.activate(allow_net_connect=False, verbose=True)
    @pytest.mark.parametrize("http_error_status", [502, 503, 504])
    def test_resilient_backend_reports_error_when_max_retries_exceeded(
        self, tmp_path, http_error_status
    ):
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
        max_retries = 5

        httpretty.register_uri(
            "GET", backend, body=json.dumps({"api_version": "1.1.0"})
        )

        # Fail one more time than the max allow retries.
        # But do add one successful request at the start, to simulate that the job was
        # in running mode at one point.
        # Namely, we want to check that it flags the job stopped with an error.
        response_list = [
            corehttpretty.Response(
                body=json.dumps(
                    {
                        "id": job_id,
                        "title": f"Job {job_id}",
                        "status": "running",
                    }
                )
            )
        ]
        response_list += [
            corehttpretty.Response(
                f"Simulate error HTTP {http_error_status}", status=http_error_status
            )
        ] * (max_retries + 1)

        httpretty.register_uri(
            "GET", f"{backend}/jobs/{job_id}", responses=response_list
        )

        root_dir = tmp_path / "job_mgr_root"
        manager = MultiBackendJobManager(poll_sleep=0.2, root_dir=root_dir)
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

        # Sanity check: the job has status "error"
        result = pd.read_csv(output_file)
        assert len(result) == 1
        assert set(result.status) == {"running"}
        assert set(result.backend_name) == {"foo"}

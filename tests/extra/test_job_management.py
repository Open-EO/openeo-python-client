import json
import os
from pathlib import Path

import pandas as pd
import pytest
import shapely.geometry.point as shpt

import openeo
from openeo.extra.job_management import MultiBackendJobManager
from openeo import BatchJob


@pytest.fixture
def temp_working_dir(tmp_path):
    """Use a temporary working directory, because some tests generate files
    in the working directory, and that pollutes your git work tree.

    Preventing it would be better, but sometimes that is difficult.
    TODO: What if we want to keep those files for inspecting after the test run, but preferably in a separate folder?
    """

    orig_workdir: Path = Path.cwd()
    # Make the CWD a bit more specific because test can use tmp_dir for other things too.
    temp_word_dir: Path = tmp_path / "test-workdir"
    if not temp_word_dir.exists():
        temp_word_dir.mkdir()
    os.chdir(temp_word_dir)

    yield temp_word_dir

    os.chdir(orig_workdir)


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

        root_dir = "job_mgr_root"
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


from time import sleep
from typing import  Union
from unittest import mock
import queue
import pandas as pd
import pytest


import openeo
import openeo.extra.job_management
from openeo.extra.job_management import (
    CsvJobDatabase,
    MultiBackendJobManager,

)
from openeo.extra.job_management.thread_worker import _JobManagerWorkerThreadPool

from openeo.rest._testing import OPENEO_BACKEND, DummyBackend, build_capabilities

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


class TestJobManagerWorkerThreadPool:

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
        year = int(row["year"])
        pg = {"yearify": {"process_id": "yearify", "arguments": {"year": year}, "result": True}}
        return connection.create_job(pg)

    @pytest.fixture
    def fresh_worker(self):
        work_queue = queue.Queue()
        result_queue = queue.Queue()
        worker = _JobManagerWorkerThreadPool(work_queue, result_queue)
        yield worker
        
        if worker.is_alive():
            worker.shutdown()

    def enqueue_and_wait(self, worker, work_item, timeout=0.2):
        worker.work_queue.put(work_item)
        worker.start()
        sleep(timeout)
        worker.shutdown()

    def test_run_jobs_collects_worker_results(self, tmp_path, job_manager, sleep_mock):
        df = pd.DataFrame({"year": [2018], "geometry": ["POINT (1 2)"]})
        job_db_path = tmp_path / "jobs.csv"
        job_db = CsvJobDatabase(job_db_path).initialize_from_df(df)

        captured_results = []
        original_process_result_queue = job_manager._process_result_queue

        def spy_process_result_queue(stats=None):
            try:
                work_result = job_manager._result_queue.get_nowait()
                captured_results.append(work_result)
            except queue.Empty:
                pass
            original_process_result_queue(stats)

        job_manager._process_result_queue = spy_process_result_queue

        run_stats = job_manager.run_jobs(job_db=job_db, start_job=self._create_year_job)

        expected = (
            _JobManagerWorkerThreadPool.WORK_TYPE_START_JOB,
            ("job-2018", True, "queued")
        )
        assert any(result == expected for result in captured_results), (
            f"Expected to see {expected!r} among worker results {captured_results!r}"
        )

    def test_worker_thread_lifecycle(self, fresh_worker):
        fresh_worker.polling_time = 0.1  
        fresh_worker.start()
        assert fresh_worker.is_alive()
        fresh_worker.shutdown()
        assert not fresh_worker.is_alive()

    def test_start_job_success(self, fresh_worker, requests_mock):
        backend_url = "https://foo.test"
        job_id = "job-123"

        requests_mock.get(backend_url, json={"api_version": "1.1.0"})
        requests_mock.post(
            f"{backend_url}/jobs",
            json={"job_id": job_id, "status": "created"},
            status_code=201,
            headers={"openeo-identifier": job_id}
        )
        requests_mock.post(
            f"{backend_url}/jobs/{job_id}/results",
            json={"job_id": job_id, "status": "finished"},
            status_code=202
        )
        requests_mock.get(
            f"{backend_url}/jobs/{job_id}",
            json={"id": job_id, "status": "finished"}
        )
        
        self.enqueue_and_wait(fresh_worker, (fresh_worker.WORK_TYPE_START_JOB, (backend_url, "bearer_token_123", job_id)))
        
        assert fresh_worker.result_queue.qsize() == 1
        work_type, (jid, success, status) = fresh_worker.result_queue.get()
        assert success is True
        assert status == "finished"

    def test_start_job_failure(self, fresh_worker, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise ConnectionError("Backend unreachable")
        monkeypatch.setattr(openeo, "connect", mock_connect)
        
        self.enqueue_and_wait(fresh_worker, (fresh_worker.WORK_TYPE_START_JOB, ("https://down.test", None, "job-456")))
        
        assert fresh_worker.result_queue.qsize() == 1
        work_type, (jid, success, error) = fresh_worker.result_queue.get()
        assert success is False
        assert "Backend unreachable" in error

    def test_bearer_token_auth(self, fresh_worker, requests_mock):
        backend_url = "https://foo.test"
        job_id = "job-789"
        
        requests_mock.post(f"{backend_url}/jobs/{job_id}/results", status_code=202)
        requests_mock.get(backend_url, json={"api_version": "1.2.0"})
        
        with mock.patch("openeo.Connection.authenticate_bearer_token") as mock_auth:
            self.enqueue_and_wait(fresh_worker, (fresh_worker.WORK_TYPE_START_JOB, (backend_url, "secret_token", job_id)))
            mock_auth.assert_called_once_with(bearer_token="secret_token")

    def test_invalid_work_type(self, fresh_worker, requests_mock, caplog):
        backend_url = "https://foo.test"
        job_id = "job-007"
        requests_mock.post(f"{backend_url}/jobs/{job_id}/results", status_code=202)
        requests_mock.get(backend_url, json={"api_version": "1.2.0"})
        
        self.enqueue_and_wait(fresh_worker, ("invalid_work_type", ("foo", "bar")))
        result = fresh_worker.result_queue.get()
        assert result[1][1] is False  # success=False
        assert "Unknown work item" in caplog.text

    def test_concurrent_processing(self, fresh_worker, requests_mock):
        for i in range(3):
            backend_url = f"https://backend{i}.test"
            job_id = f"job-{i}"
            requests_mock.post(f"{backend_url}/jobs/{job_id}/results", status_code=202)
            fresh_worker.work_queue.put((fresh_worker.WORK_TYPE_START_JOB, (backend_url, None, job_id)))
        
        fresh_worker.start()
        sleep(0.5)
        fresh_worker.shutdown()
        assert fresh_worker.result_queue.qsize() == 3

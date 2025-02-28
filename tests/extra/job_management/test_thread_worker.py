
from collections import defaultdict
import logging
from openeo.extra.job_management.thread_worker import _JobManagerWorkerThreadPool


class TestJobManagerWorkerThreadPool:


    def test_worker_thread_lifecycle(self, caplog):
        worker = _JobManagerWorkerThreadPool()
        assert not worker.executor._shutdown 
        
        # Set log level to INFO and capture logs
        with caplog.at_level(logging.INFO):
             # Verify executor is running
            worker.shutdown()
        assert worker.executor._shutdown
        assert "Shutting down worker thread pool" in caplog.text

    def test_start_job_success(self, requests_mock):
        worker = _JobManagerWorkerThreadPool()
        stats = defaultdict(int)

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
        for i in range(3):
            worker.submit_work(worker.WORK_TYPE_START_JOB, (backend_url, "token", job_id))
        worker.process_futures(stats)
        worker.shutdown()
        assert stats["job start"] == 3
        assert len(worker.futures) == 0 

    def test_start_job_failure(self, requests_mock, caplog):
        worker = _JobManagerWorkerThreadPool()
        stats = defaultdict(int)

        backend_url = "https://down.test"
        job_id = "job-123"

        requests_mock.get(backend_url, exc=ConnectionError("Backend unreachable"))

        worker.submit_work(worker.WORK_TYPE_START_JOB, (backend_url, "token", job_id))
        worker.process_futures(stats)
        worker.shutdown()

        assert stats["job start failed"] == 1
        assert f"Job {job_id} failed: Backend unreachable" in caplog.text
        


    def test_invalid_work_type(self, requests_mock, caplog):
        worker = _JobManagerWorkerThreadPool()
        stats = defaultdict(int)

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
        
        worker.submit_work("invalid_work_type", (backend_url, "token", job_id))
        worker.process_futures(stats)
        worker.shutdown()
        assert "Unknown work type: invalid_work_type" in caplog.text

    



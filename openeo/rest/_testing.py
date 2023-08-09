import re

from openeo import Connection


class DummyBackend:
    """
    Dummy backend that handles sync/batch execution requests
    and allows inspection of posted process graphs
    """

    # Default result (can serve both as JSON or binary data)
    DEFAULT_RESULT = b'{"what?": "Result data"}'

    def __init__(self, requests_mock, connection: Connection):
        self.connection = connection
        self.sync_requests = []
        self.batch_jobs = {}
        self.next_result = self.DEFAULT_RESULT
        requests_mock.post(connection.build_url("/result"), content=self._handle_post_result)
        requests_mock.post(connection.build_url("/jobs"), content=self._handle_post_jobs)
        requests_mock.post(
            re.compile(connection.build_url(r"/jobs/(job-\d+)/results$")), content=self._handle_post_job_results
        )
        requests_mock.get(re.compile(connection.build_url(r"/jobs/(job-\d+)$")), json=self._handle_get_job)
        requests_mock.get(
            re.compile(connection.build_url(r"/jobs/(job-\d+)/results$")), json=self._handle_get_job_results
        )
        requests_mock.get(
            re.compile(connection.build_url("/jobs/(.*?)/results/result.data$")),
            content=self._handle_get_job_result_asset,
        )

    def _handle_post_result(self, request, context):
        """handler of `POST /result` (synchronous execute)"""
        pg = request.json()["process"]["process_graph"]
        self.sync_requests.append(pg)
        return self.next_result

    def _handle_post_jobs(self, request, context):
        """handler of `POST /jobs` (create batch job)"""
        pg = request.json()["process"]["process_graph"]
        job_id = f"job-{len(self.batch_jobs):03d}"
        self.batch_jobs[job_id] = {"job_id": job_id, "pg": pg, "status": "created"}
        context.status_code = 201
        context.headers["openeo-identifier"] = job_id

    def _get_job_id(self, request) -> str:
        match = re.match(r"^/jobs/(job-\d+)(/|$)", request.path)
        if not match:
            raise ValueError(f"Failed to extract job_id from {request.path}")
        job_id = match.group(1)
        assert job_id in self.batch_jobs
        return job_id

    def _handle_post_job_results(self, request, context):
        """Handler of `POST /job/{job_id}/results` (start batch job)."""
        job_id = self._get_job_id(request)
        assert self.batch_jobs[job_id]["status"] == "created"
        # TODO: support custom status sequence (instead of directly going to status "finished")?
        self.batch_jobs[job_id]["status"] = "finished"
        context.status_code = 202

    def _handle_get_job(self, request, context):
        """Handler of `GET /job/{job_id}` (get batch job status and metadata)."""
        job_id = self._get_job_id(request)
        return {"id": job_id, "status": self.batch_jobs[job_id]["status"]}

    def _handle_get_job_results(self, request, context):
        """Handler of `GET /job/{job_id}/results` (list batch job results)."""
        job_id = self._get_job_id(request)
        assert self.batch_jobs[job_id]["status"] == "finished"
        return {
            "id": job_id,
            "assets": {"result.data": {"href": self.connection.build_url(f"/jobs/{job_id}/results/result.data")}},
        }

    def _handle_get_job_result_asset(self, request, context):
        """Handler of `GET /job/{job_id}/results/result.data` (get batch job result asset)."""
        job_id = self._get_job_id(request)
        assert self.batch_jobs[job_id]["status"] == "finished"
        return self.next_result

    def get_sync_pg(self) -> dict:
        """Get one and only synchronous process graph"""
        assert len(self.sync_requests) == 1
        return self.sync_requests[0]

    def get_batch_pg(self) -> dict:
        """Get one and only batch process graph"""
        assert len(self.batch_jobs) == 1
        return self.batch_jobs[max(self.batch_jobs.keys())]["pg"]

    def get_pg(self) -> dict:
        """Get one and only batch process graph (sync or batch)"""
        pgs = self.sync_requests + [b["pg"] for b in self.batch_jobs.values()]
        assert len(pgs) == 1
        return pgs[0]

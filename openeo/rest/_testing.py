from __future__ import annotations

import collections
import json
import re
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from openeo import Connection, DataCube
from openeo.rest.vectorcube import VectorCube

OPENEO_BACKEND = "https://openeo.test/"


class OpeneoTestingException(Exception):
    pass


class DummyBackend:
    """
    Dummy backend that handles sync/batch execution requests
    and allows inspection of posted process graphs
    """

    # TODO: move to openeo.testing
    # TODO: unify "batch_jobs", "batch_jobs_full" and "extra_job_metadata_fields"?
    # TODO: unify "sync_requests" and "sync_requests_full"?

    __slots__ = (
        "_requests_mock",
        "connection",
        "file_formats",
        "sync_requests",
        "sync_requests_full",
        "batch_jobs",
        "batch_jobs_full",
        "validation_requests",
        "next_result",
        "next_validation_errors",
        "_forced_job_status",
        "job_status_updater",
        "job_id_generator",
        "extra_job_metadata_fields",
    )

    # Default result (can serve both as JSON or binary data)
    DEFAULT_RESULT = b'{"what?": "Result data"}'

    def __init__(
        self,
        requests_mock,
        connection: Connection,
    ):
        self._requests_mock = requests_mock
        self.connection = connection
        self.file_formats = {"input": {}, "output": {}}
        self.sync_requests = []
        self.sync_requests_full = []
        self.batch_jobs = {}
        self.batch_jobs_full = {}
        self.validation_requests = []
        self.next_result = self.DEFAULT_RESULT
        self.next_validation_errors = []
        self.extra_job_metadata_fields = []
        self._forced_job_status: Dict[str, str] = {}

        # Job status update hook:
        #   callable that is called on starting a job, and getting job metadata
        #   allows to dynamically change how the status of a job evolves
        #   By default: immediately set to "finished" once job is started
        self.job_status_updater = lambda job_id, current_status: "finished"

        # Optional job id generator hook:
        #   callable that generates a job id, e.g. based on the process graph.
        #   When set to None, or the callable returns None, or it returns an existing job id:
        #   things fall back to auto-increment job ids ("job-000", "job-001", "job-002", ...)
        self.job_id_generator: Optional[Callable[[dict], str]] = None

        requests_mock.post(
            connection.build_url("/result"),
            content=self._handle_post_result,
        )
        requests_mock.post(
            connection.build_url("/jobs"),
            content=self._handle_post_jobs,
        )
        requests_mock.post(
            re.compile(connection.build_url(r"/jobs/(job-\d+)/results$")), content=self._handle_post_job_results
        )
        requests_mock.get(re.compile(connection.build_url(r"/jobs/(job-\d+)$")), json=self._handle_get_job)
        requests_mock.get(
            re.compile(connection.build_url(r"/jobs/(job-\d+)/results$")), json=self._handle_get_job_results
        )
        requests_mock.delete(
            re.compile(connection.build_url(r"/jobs/(job-\d+)/results$")), json=self._handle_delete_job_results
        )
        requests_mock.get(
            re.compile(connection.build_url("/jobs/(.*?)/results/result.data$")),
            content=self._handle_get_job_result_asset,
        )
        requests_mock.get(
            re.compile(connection.build_url(r"/jobs/(.*?)/logs($|\?.*)")),
            # TODO: need to fine-tune dummy logs?
            json={"logs": [], "links": []},
        )
        requests_mock.post(connection.build_url("/validation"), json=self._handle_post_validation)

    @classmethod
    def at_url(cls, root_url: str, *, requests_mock, capabilities: Optional[dict] = None) -> DummyBackend:
        """
        Factory to build dummy backend from given root URL
        including creation of connection and mocking of capabilities doc
        """
        root_url = root_url.rstrip("/") + "/"
        requests_mock.get(root_url, json=build_capabilities(**(capabilities or {})))
        connection = Connection(root_url)
        return cls(requests_mock=requests_mock, connection=connection)

    def setup_collection(
        self,
        collection_id: str,
        *,
        temporal: Union[bool, Tuple[str, str]] = True,
        bands: Sequence[str] = ("B1", "B2", "B3"),
    ):
        # TODO: also mock `/collections` overview
        # TODO: option to override cube_dimensions as a whole, or override dimension names
        cube_dimensions = {
            "x": {"type": "spatial"},
            "y": {"type": "spatial"},
        }

        if temporal:
            cube_dimensions["t"] = {
                "type": "temporal",
                "extent": temporal if isinstance(temporal, tuple) else [None, None],
            }
        if bands:
            cube_dimensions["bands"] = {"type": "bands", "values": list(bands)}

        self._requests_mock.get(
            self.connection.build_url(f"/collections/{collection_id}"),
            # TODO: add more metadata?
            json={
                "id": collection_id,
                # define temporal  and band dim
                "cube:dimensions": cube_dimensions,
            },
        )
        return self

    def setup_file_format(self, name: str, type: str = "output", gis_data_types: Iterable[str] = ("raster",)):
        self.file_formats[type][name] = {
            "title": name,
            "gis_data_types": list(gis_data_types),
            "parameters": {},
        }
        self._requests_mock.get(self.connection.build_url("/file_formats"), json=self.file_formats)
        return self

    def _handle_post_result(self, request, context):
        """handler of `POST /result` (synchronous execute)"""
        post_data = request.json()
        pg = post_data["process"]["process_graph"]
        self.sync_requests_full.append(post_data)
        self.sync_requests.append(pg)
        result = self.next_result
        if isinstance(result, (dict, list)):
            result = json.dumps(result).encode("utf-8")
        elif isinstance(result, str):
            result = result.encode("utf-8")
        assert isinstance(result, bytes)
        return result

    def _handle_post_jobs(self, request, context):
        """handler of `POST /jobs` (create batch job)"""
        post_data = request.json()
        pg = post_data["process"]["process_graph"]

        # Generate (new) job id
        job_id = self.job_id_generator and self.job_id_generator(process_graph=pg)
        if not job_id or job_id in self.batch_jobs:
            # As fallback: use auto-increment job ids ("job-000", "job-001", "job-002", ...)
            job_id = f"job-{len(self.batch_jobs):03d}"
        assert job_id not in self.batch_jobs

        # Full post data dump
        self.batch_jobs_full[job_id] = post_data

        # Batch job essentials
        job_data = {"job_id": job_id, "pg": pg, "status": "created"}
        for field in ["title", "description"]:
            if field in post_data:
                job_data[field] = post_data[field]
        for field in self.extra_job_metadata_fields:
            job_data[field] = post_data.get(field)
        self.batch_jobs[job_id] = job_data
        context.status_code = 201
        context.headers["openeo-identifier"] = job_id

    def _get_job_id(self, request) -> str:
        match = re.match(r"^/jobs/(job-\d+)(/|$)", request.path)
        if not match:
            raise OpeneoTestingException(f"Failed to extract job_id from {request.path}")
        job_id = match.group(1)
        assert job_id in self.batch_jobs
        return job_id

    def _get_job_status(self, job_id: str, current_status: str) -> str:
        if job_id in self._forced_job_status:
            return self._forced_job_status[job_id]
        return self.job_status_updater(job_id=job_id, current_status=current_status)

    def _handle_post_job_results(self, request, context):
        """Handler of `POST /job/{job_id}/results` (start batch job)."""
        job_id = self._get_job_id(request)
        assert self.batch_jobs[job_id]["status"] == "created"
        self.batch_jobs[job_id]["status"] = self._get_job_status(
            job_id=job_id, current_status=self.batch_jobs[job_id]["status"]
        )
        context.status_code = 202

    def _handle_get_job(self, request, context):
        """Handler of `GET /job/{job_id}` (get batch job status and metadata)."""
        job_id = self._get_job_id(request)
        # Allow updating status with `job_status_setter` once job got past status "created"
        if self.batch_jobs[job_id]["status"] != "created":
            self.batch_jobs[job_id]["status"] = self._get_job_status(
                job_id=job_id, current_status=self.batch_jobs[job_id]["status"]
            )
        result = {
            # TODO: add some more required fields like "process" and "created"?
            "id": job_id,
            "status": self.batch_jobs[job_id]["status"],
        }
        if self.batch_jobs[job_id]["status"] == "finished":  # HACK some realistic values for a small job
            result["costs"] = 123
            result["usage"] = {
                "cpu": {"unit": "cpu-seconds", "value": 1234.5},
                "memory": {"unit": "mb-seconds", "value": 34567.89},
                "duration": {"unit": "seconds", "value": 2345},
            }
        return result

    def _handle_get_job_results(self, request, context):
        """Handler of `GET /job/{job_id}/results` (list batch job results)."""
        job_id = self._get_job_id(request)
        assert self.batch_jobs[job_id]["status"] == "finished"
        return {
            "id": job_id,
            "assets": {"result.data": {"href": self.connection.build_url(f"/jobs/{job_id}/results/result.data")}},
        }

    def _handle_delete_job_results(self, request, context):
        """Handler of `DELETE /job/{job_id}/results` (cancel job)."""
        job_id = self._get_job_id(request)
        self.batch_jobs[job_id]["status"] = "canceled"
        self._forced_job_status[job_id] = "canceled"
        context.status_code = 204

    def _handle_get_job_result_asset(self, request, context):
        """Handler of `GET /job/{job_id}/results/result.data` (get batch job result asset)."""
        job_id = self._get_job_id(request)
        assert self.batch_jobs[job_id]["status"] == "finished"
        return self.next_result

    def _handle_post_validation(self, request, context):
        """Handler of `POST /validation` (validate process graph)."""
        pg = request.json()["process_graph"]
        self.validation_requests.append(pg)
        return {"errors": self.next_validation_errors}

    def get_sync_pg(self) -> dict:
        """Get one and only synchronous process graph"""
        assert len(self.sync_requests) == 1
        return self.sync_requests[0]

    def get_sync_post_data(self) -> dict:
        """Get post data of the one and only synchronous job."""
        assert len(self.sync_requests_full) == 1
        return self.sync_requests_full[0]

    def get_batch_pg(self) -> dict:
        """
        Get process graph of the one and only batch job.
        Fails when there is none or more than one.
        """
        assert len(self.batch_jobs) == 1
        return self.batch_jobs[max(self.batch_jobs.keys())]["pg"]

    def get_batch_post_data(self) -> dict:
        """
        Get post data of the one and only batch job.
        Fails when there is none or more than one.
        """
        assert len(self.batch_jobs_full) == 1
        return self.batch_jobs_full[max(self.batch_jobs_full.keys())]

    def get_validation_pg(self) -> dict:
        """
        Get process graph of the one and only validation request.
        """
        assert len(self.validation_requests) == 1
        return self.validation_requests[0]

    def get_pg(self, process_id: Optional[str] = None) -> dict:
        """
        Get one and only batch process graph (sync or batch)

        :param process_id: just return single process graph node with this process_id
        :return: process graph (flat graph representation) or process graph node
        """
        pgs = self.sync_requests + [b["pg"] for b in self.batch_jobs.values()]
        if len(pgs) != 1:
            raise OpeneoTestingException(f"Expected single process graph, but collected {len(pgs)}")
        pg = pgs[0]
        if process_id:
            # Just return single node (by process_id)
            found = [node for node in pg.values() if node.get("process_id") == process_id]
            if len(found) != 1:
                raise OpeneoTestingException(
                    f"Expected single process graph node with process_id {process_id!r}, but found {len(found)}: {found}"
                )
            return found[0]
        return pg

    def execute(self, cube: Union[DataCube, VectorCube], process_id: Optional[str] = None) -> dict:
        """
        Execute given cube (synchronously) and return observed process graph (or subset thereof).

        :param cube: cube to execute on dummy back-end
        :param process_id: just return single process graph node with this process_id
        :return: process graph (flat graph representation) or process graph node
        """
        cube.execute()
        return self.get_pg(process_id=process_id)

    def setup_simple_job_status_flow(
        self,
        *,
        queued: int = 1,
        running: int = 4,
        final: str = "finished",
        final_per_job: Optional[Mapping[str, str]] = None,
    ):
        """
        Set up simple job status flow:

            queued (a couple of times) -> running (a couple of times) -> finished/error.

        Final state can be specified generically with arg `final`
        and, optionally, further fine-tuned per job with `final_per_job`.
        """
        template = ["queued"] * queued + ["running"] * running
        job_stacks = collections.defaultdict(template.copy)
        final_per_job = final_per_job or {}

        def get_status(job_id: str, current_status: str) -> str:
            stack = job_stacks[job_id]
            # Pop first item each time, unless we're in final state
            return stack.pop(0) if len(stack) > 0 else final_per_job.get(job_id, final)

        self.job_status_updater = get_status


def build_capabilities(
    *,
    api_version: str = "1.0.0",
    stac_version: str = "0.9.0",
    basic_auth: bool = True,
    oidc_auth: bool = True,
    collections: bool = True,
    processes: bool = True,
    sync_processing: bool = True,
    validation: bool = False,
    batch_jobs: bool = True,
    udp: bool = False,
) -> dict:
    """Build a dummy capabilities document for testing purposes."""

    endpoints = []
    if basic_auth:
        endpoints.append({"path": "/credentials/basic", "methods": ["GET"]})
    if oidc_auth:
        endpoints.append({"path": "/credentials/oidc", "methods": ["GET"]})
    if basic_auth or oidc_auth:
        endpoints.append({"path": "/me", "methods": ["GET"]})

    if collections:
        endpoints.append({"path": "/collections", "methods": ["GET"]})
        endpoints.append({"path": "/collections/{collection_id}", "methods": ["GET"]})
    if processes:
        endpoints.append({"path": "/processes", "methods": ["GET"]})
    if sync_processing:
        endpoints.append({"path": "/result", "methods": ["POST"]})
    if validation:
        endpoints.append({"path": "/validation", "methods": ["POST"]})
    if batch_jobs:
        endpoints.extend(
            [
                {"path": "/jobs", "methods": ["GET", "POST"]},
                {"path": "/jobs/{job_id}", "methods": ["GET", "DELETE"]},
                {"path": "/jobs/{job_id}/results", "methods": ["GET", "POST", "DELETE"]},
                {"path": "/jobs/{job_id}/logs", "methods": ["GET"]},
            ]
        )
    if udp:
        endpoints.extend(
            [
                {"path": "/process_graphs", "methods": ["GET"]},
                {"path": "/process_graphs/{process_graph_id", "methods": ["GET", "PUT", "DELETE"]},
            ]
        )

    capabilities = {
        "api_version": api_version,
        "stac_version": stac_version,
        "id": "dummy",
        "title": "Dummy openEO back-end",
        "description": "Dummy openeEO back-end",
        "endpoints": endpoints,
        "links": [],
    }
    return capabilities

import re
from pathlib import Path

import pytest

from openeo import Connection
from openeo.internal.graph_building import PGNode
from openeo.rest.vectorcube import VectorCube


@pytest.fixture
def vector_cube(con100) -> VectorCube:
    pgnode = PGNode(process_id="create_vector_cube")
    return VectorCube(graph=pgnode, connection=con100)


class DummyBackend:
    """
    Dummy backend that handles sync/batch execution requests
    and allows inspection of posted process graphs
    """

    def __init__(self, requests_mock, connection: Connection):
        self.connection = connection
        self.sync_requests = []
        self.batch_jobs = {}
        self.next_result = b"Result data"
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


@pytest.fixture
def dummy_backend(requests_mock, con100) -> DummyBackend:
    yield DummyBackend(requests_mock=requests_mock, connection=con100)


def test_raster_to_vector(con100):
    img = con100.load_collection("S2")
    vector_cube = img.raster_to_vector()
    vector_cube_tranformed = vector_cube.run_udf(udf="python source code", runtime="Python")

    assert vector_cube_tranformed.flat_graph() == {
        'loadcollection1': {
            'arguments': {
                'id': 'S2',
                'spatial_extent': None,
                'temporal_extent': None
            },
            'process_id': 'load_collection'
        },
        'rastertovector1': {
            'arguments': {
                'data': {'from_node': 'loadcollection1'}
            },
            'process_id': 'raster_to_vector'
        },
        'runudf1': {
            'arguments': {
                'data': {'from_node': 'rastertovector1'},
                'runtime': 'Python',
                'udf': 'python source code'
            },
            'process_id': 'run_udf',
            'result': True}
    }


@pytest.mark.parametrize(
    ["filename", "expected_format"],
    [
        ("result.json", "JSON"),  # TODO possible to allow "GeoJSON" with ".json" extension?
        ("result.geojson", "GeoJSON"),
        ("result.nc", "netCDF"),
    ],
)
@pytest.mark.parametrize("path_class", [str, Path])
@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_download_auto_save_result_only_file(
    vector_cube, dummy_backend, tmp_path, filename, expected_format, path_class, exec_mode
):
    output_path = tmp_path / filename
    if exec_mode == "sync":
        vector_cube.download(path_class(output_path))
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=path_class(output_path))
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "createvectorcube1"},
                "format": expected_format,
                "options": {},
            },
            "result": True,
        },
    }
    assert output_path.read_bytes() == b"Result data"


@pytest.mark.parametrize(
    ["filename", "format", "expected_format"],
    [
        ("result.json", "JSON", "JSON"),
        ("result.geojson", "GeoJSON", "GeoJSON"),
        ("result.nc", "netCDF", "netCDF"),
        ("result.nc", "NETcDf", "NETcDf"),  # TODO #401 normalize format
        ("result.nc", "inV6l1d!!!", "inV6l1d!!!"),  # TODO #401 this should fail?
        ("result.json", None, "JSON"),
        ("result.geojson", None, "GeoJSON"),
        ("result.nc", None, "netCDF"),
        # TODO #449 more formats to autodetect?
    ],
)
@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_download_auto_save_result_with_format(
    vector_cube, dummy_backend, tmp_path, filename, format, expected_format, exec_mode
):
    output_path = tmp_path / filename
    if exec_mode == "sync":
        vector_cube.download(output_path, format=format)
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=output_path, out_format=format)
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "createvectorcube1"},
                "format": expected_format,
                "options": {},
            },
            "result": True,
        },
    }
    assert output_path.read_bytes() == b"Result data"


@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_download_auto_save_result_with_options(vector_cube, dummy_backend, tmp_path, exec_mode):
    output_path = tmp_path / "result.json"
    format = "GeoJSON"
    options = {"precision": 7}

    if exec_mode == "sync":
        vector_cube.download(output_path, format=format, options=options)
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=output_path, out_format=format, **options)
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "createvectorcube1"},
                "format": "GeoJSON",
                "options": {"precision": 7},
            },
            "result": True,
        },
    }
    assert output_path.read_bytes() == b"Result data"


@pytest.mark.parametrize(
    ["output_file", "format", "expected_format"],
    [
        ("result.geojson", None, "GeoJSON"),
        ("result.geojson", "GeoJSON", "GeoJSON"),
        ("result.json", "JSON", "JSON"),
        ("result.nc", "netCDF", "netCDF"),
    ],
)
@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_save_result_and_download(
    vector_cube, dummy_backend, tmp_path, output_file, format, expected_format, exec_mode
):
    """e.g. https://github.com/Open-EO/openeo-geopyspark-driver/issues/477"""
    vector_cube = vector_cube.save_result(format=format)
    output_path = tmp_path / output_file
    if exec_mode == "sync":
        vector_cube.download(output_path)
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=output_path)
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "createvectorcube1"}, "format": expected_format, "options": {}},
            "result": True,
        },
    }
    assert output_path.read_bytes() == b"Result data"

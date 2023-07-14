from typing import List

import pytest

from openeo.internal.graph_building import PGNode
from openeo.rest.vectorcube import VectorCube


@pytest.fixture
def vector_cube(con100) -> VectorCube:
    pgnode = PGNode(process_id="create_vector_cube")
    return VectorCube(graph=pgnode, connection=con100)


class DownloadSpy:
    """
    Test helper to track download requests and optionally override next response to return.
    """

    __slots__ = ["requests", "next_response"]

    def __init__(self):
        self.requests: List[dict] = []
        self.next_response: bytes = b"Spy data"

    @property
    def only_request(self) -> dict:
        """Get progress graph of only request done"""
        assert len(self.requests) == 1
        return self.requests[-1]

    @property
    def last_request(self) -> dict:
        """Get last progress graph"""
        assert len(self.requests) > 0
        return self.requests[-1]


@pytest.fixture
def download_spy(requests_mock, con100) -> DownloadSpy:
    """Test fixture to spy on (and mock) `POST /result` (download) requests."""
    spy = DownloadSpy()

    def post_result(request, context):
        pg = request.json()["process"]["process_graph"]
        spy.requests.append(pg)
        return spy.next_response

    requests_mock.post(con100.build_url("/result"), content=post_result)
    yield spy


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
        ("result.json", "GeoJSON"),  # TODO #401 possible to detect "GeoJSON from ".json" extension?
        ("result.geojson", "GeoJSON"),
        ("result.nc", "GeoJSON"),  # TODO #401 autodetect format from filename
    ],
)
def test_download_auto_save_result_only_file(vector_cube, download_spy, tmp_path, filename, expected_format):
    vector_cube.download(tmp_path / filename)

    assert download_spy.only_request == {
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
    assert (tmp_path / filename).read_bytes() == b"Spy data"


@pytest.mark.parametrize(
    ["filename", "format", "expected_format"],
    [
        ("result.json", "JSON", "JSON"),
        ("result.geojson", "GeoJSON", "GeoJSON"),
        ("result.nc", "netCDF", "netCDF"),
        # TODO #401 more formats to autodetect?
        ("result.nc", "NETcDf", "NETcDf"),  # TODO #401 normalize format
        ("result.nc", "inV6l1d!!!", "inV6l1d!!!"),  # TODO #401 this should fail
        ("result.json", None, None),  # TODO #401 autodetect format from filename
        ("result.nc", None, None),  # TODO #401 autodetect format from filename
    ],
)
def test_download_auto_save_result_with_format(vector_cube, download_spy, tmp_path, filename, format, expected_format):
    vector_cube.download(tmp_path / filename, format=format)

    assert download_spy.only_request == {
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
    assert (tmp_path / filename).read_bytes() == b"Spy data"


def test_download_auto_save_result_with_options(vector_cube, download_spy, tmp_path):
    vector_cube.download(tmp_path / "result.json", format="GeoJSON", options={"precision": 7})

    assert download_spy.only_request == {
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
    assert (tmp_path / "result.json").read_bytes() == b"Spy data"


def test_save_result_and_download(vector_cube, download_spy, tmp_path):
    """e.g. https://github.com/Open-EO/openeo-geopyspark-driver/issues/477"""
    vector_cube = vector_cube.save_result(format="JSON")
    vector_cube.download(tmp_path / "result.json")
    # TODO #401 there should only be one save_result node
    assert download_spy.only_request == {
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "createvectorcube1"}, "format": "JSON", "options": {}},
        },
        "saveresult2": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "saveresult1"}, "format": "GeoJSON", "options": {}},
            "result": True,
        },
    }
    assert (tmp_path / "result.json").read_bytes() == b"Spy data"

import pytest
import shapely.geometry

import openeo
from openeo.rest.connection import Connection

API_URL = "https://oeo.net"


@pytest.fixture
def con100(requests_mock) -> Connection:
    """
    Fixture to have a v1.0.0 connection to a backend
    with a some default image collections
    """
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    requests_mock.get(API_URL + "/collections/S2", json={
        "properties": {
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["B2", "B3"]}
            },
            "eo:bands": [
                {"name": "B2", "common_name": "blue"},
                {"name": "B3", "common_name": "green"},
            ]
        }
    })
    requests_mock.get(API_URL + "/collections/MASK", json={})
    return openeo.connect(API_URL)


def test_mask_polygon(con100: Connection):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.mask(polygon=polygon)
    assert sorted(masked.graph.keys()) == ["loadcollection1", "mask1"]
    assert masked.graph["mask1"] == {
        "process_id": "mask",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            'mask': {
                'coordinates': (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
                'crs': {'properties': {'name': 'EPSG:4326'}, 'type': 'name'},
                'type': 'Polygon'}
        },
        "result": True
    }


def test_mask_polygon_path(con100: Connection):
    img = con100.load_collection("S2")
    masked = img.mask(polygon="path/to/polygon.json")
    assert sorted(masked.graph.keys()) == ["loadcollection1", "mask1", "readvector1"]
    assert masked.graph["mask1"] == {
        "process_id": "mask",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "readvector1"},
        },
        "result": True
    }
    assert masked.graph["readvector1"] == {
        "process_id": "read_vector",
        "arguments": {"filename": "path/to/polygon.json"},
    }


def test_mask_raster(con100: Connection):
    img = con100.load_collection("S2")
    mask = con100.load_collection("MASK")
    masked = img.mask(rastermask=mask, replacement=102)
    assert masked.graph["mask1"] == {
        "process_id": "mask",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "loadcollection2"},
            "replacement": 102
        },
        "result": True
    }


def test_merge_cubes(con100: Connection):
    a = con100.load_collection("S2")
    b = con100.load_collection("MASK")
    c = a.merge(b)
    assert c.graph["mergecubes1"] == {
        "process_id": "merge_cubes",
        "arguments": {
            "cube1": {"from_node": "loadcollection1"},
            "cube2": {"from_node": "loadcollection2"},
        },
        "result": True
    }


def test_ndvi_simple(con100: Connection):
    ndvi = con100.load_collection("S2").ndvi()
    assert sorted(ndvi.graph.keys()) == ["loadcollection1", "ndvi1"]
    assert ndvi.graph["ndvi1"] == {
        "process_id": "ndvi",
        "arguments": {"data": {"from_node": "loadcollection1"}},
        "result": True,
    }


def test_ndvi_args(con100: Connection):
    ndvi = con100.load_collection("S2").ndvi(nir="nirr", red="rred", target_band="ndvii")
    assert sorted(ndvi.graph.keys()) == ["loadcollection1", "ndvi1"]
    assert ndvi.graph["ndvi1"] == {
        "process_id": "ndvi",
        "arguments": {"data": {"from_node": "loadcollection1"}, "nir": "nirr", "red": "rred", "target_band": "ndvii"},
        "result": True,
    }


def test_viewing_service(con100: Connection, requests_mock):
    def check_request(request):
        assert request.json() == {
            'custom_param': 45,
            'description': 'Nice!',
            'process_graph': {
                'loadcollection1': {
                    'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
                    'process_id': 'load_collection',
                    'result': True,
                }
            },
            'title': 'S2 Foo',
            'type': 'WMTS',
        }
        return True

    requests_mock.post(
        API_URL + "/services",
        status_code=201,
        text='',
        headers={'Location': API_URL + "/_s/sf00", 'OpenEO-Identifier': 'sf00'},
        additional_matcher=check_request
    )

    img = con100.load_collection("S2")
    res = img.tiled_viewing_service(type="WMTS", title="S2 Foo", description="Nice!", custom_param=45)
    assert res == {"url": API_URL + "/_s/sf00", 'service_id': 'sf00'}

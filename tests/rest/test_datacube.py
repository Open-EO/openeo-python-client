import openeo
import pytest
from openeo.internal.graphbuilder import GraphBuilder
from openeo.rest.connection import Connection

API_URL = "https://oeo.net"


@pytest.fixture
def con100(requests_mock) -> Connection:
    """
    Fixture to have a v1.0.0 connection to a backend with a default image collection
    """
    GraphBuilder.id_counter = {}
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
    return openeo.connect(API_URL)


def test_mask_raster(con100: Connection):
    img = con100.load_collection("S2")
    mask = con100.load_collection("S2")
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

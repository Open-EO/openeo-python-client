import pytest

import openeo
from openeo.imagecollection import CollectionMetadata
from openeo.graphbuilder import GraphBuilder
from openeo.rest.imagecollectionclient import ImageCollectionClient

API_URL = "http://localhost:8000/api"


@pytest.fixture
def session040(requests_mock):
    session = openeo.connect(API_URL)
    requests_mock.get(API_URL + "/", json={"version": "0.4.0"})
    return session


def test_metadata_from_api(session040, requests_mock):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    metadata = session040.collection_metadata("SENTINEL2")
    assert metadata.get("foo") == "bar"


def test_metadata_load_collection(session040, requests_mock):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={
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
    im = ImageCollectionClient.load_collection('SENTINEL2', session=session040)
    assert im.metadata.bands == [
        CollectionMetadata.Band("B2", "blue", None),
        CollectionMetadata.Band("B3", "green", None)
    ]


def test_empty_mask():
    from shapely import geometry
    polygon = geometry.Polygon([[1.0, 1.0], [2.0, 1.0], [2.0, 1.0], [1.0, 1.0]])

    client = ImageCollectionClient(node_id=None, builder=GraphBuilder(), session=None)

    with pytest.raises(ValueError, match=r"Mask .+ has an area of 0.0"):
        client.mask(polygon)

import pathlib

import pytest

import openeo
from openeo.internal.graphbuilder_040 import GraphBuilder
from openeo.imagecollection import CollectionMetadata
from openeo.rest.imagecollectionclient import ImageCollectionClient

API_URL = "https://oeo.net"


@pytest.fixture
def session040(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "0.4.0"})
    session = openeo.connect(API_URL)
    return session


def test_metadata_from_api(session040, requests_mock):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    metadata = session040.collection_metadata("SENTINEL2")
    assert metadata.get("foo") == "bar"


def test_metadata_load_collection_040(session040, requests_mock):
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


def test_download(session040, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + '/result', text="tiffdata")
    path = tmpdir.join("tmp.tiff")
    session040.load_collection("SENTINEL2").download(str(path), format="GTIFF")
    assert path.read() == "tiffdata"


def test_download_pathlib(session040, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + '/result', text="tiffdata")
    path = tmpdir.join("tmp.tiff")
    session040.load_collection("SENTINEL2").download(pathlib.Path(str(path)), format="GTIFF")
    assert path.read() == "tiffdata"


def test_download_with_bearer_token(session040, requests_mock, tmpdir):
    """https://github.com/Open-EO/openeo-python-client/issues/95"""
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.get(API_URL + '/credentials/basic', json={"access_token": "w3lc0m3"})
    session040.authenticate_basic("test", "test123")

    def result_callback(request, context):
        assert request.headers["Authorization"] == "Bearer w3lc0m3"
        return "tiffdata"

    requests_mock.post(API_URL + '/result', text=result_callback)
    path = tmpdir.join("tmp.tiff")
    session040.load_collection("SENTINEL2").download(str(path), format="GTIFF")
    assert path.read() == "tiffdata"

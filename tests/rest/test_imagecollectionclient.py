import pytest

import openeo
from openeo.rest.imagecollectionclient import CollectionMetadata, ImageCollectionClient

API_URL = "http://localhost:8000/api"


@pytest.fixture
def session040(requests_mock):
    session = openeo.connect(API_URL)
    requests_mock.get(API_URL + "/", json={"version": "0.4.0"})
    return session


def test_metadata_from_api(session040, requests_mock):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    metadata = CollectionMetadata.from_api(session040, "SENTINEL2")
    assert metadata.get("foo") == "bar"


def test_metadata_get():
    metadata = CollectionMetadata({
        "foo": "bar",
        "very": {
            "deeply": {"nested": {"path": {"to": "somewhere"}}}
        }
    })
    assert metadata.get("foo") == "bar"
    assert metadata.get("very", "deeply", "nested", "path", "to") == "somewhere"
    assert metadata.get("invalid", "key") is None
    assert metadata.get("invalid", "key", default="nope") == "nope"


def test_metadata_extent():
    metadata = CollectionMetadata({
        "extent": {"spatial": {"xmin": 4, "xmax": 10}}
    })
    assert metadata.extent == {"spatial": {"xmin": 4, "xmax": 10}}


def test_bands_eo_bands():
    metadata = CollectionMetadata({
        "properties": {"eo:bands": [
            {"name": "foo", "common_name": "F00"},
            {"name": "bar"}
        ]}
    })
    assert metadata.band_names == ["foo", "bar"]
    assert metadata.get_band_info() == [("foo", "F00"), ("bar", None)]


def test_bands_cube_dimensions():
    metadata = CollectionMetadata({
        "properties": {"cube:dimensions": {
            "x": {"type": "spatial", "axis": "x"},
            "b": {"type": "bands", "values": ["foo", "bar"]}
        }}
    })
    assert metadata.band_names == ["foo", "bar"]
    assert metadata.get_band_info() == [("foo", None), ("bar", None)]


def test_load_collection(session040, requests_mock):
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
    assert im.bands == ["B2", "B3"]
    assert im.metadata.get_band_info() == [("B2", "blue"), ("B3", "green")]

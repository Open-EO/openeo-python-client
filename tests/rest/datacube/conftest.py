from typing import List

import pytest

import openeo
from openeo.rest.connection import Connection
from openeo.rest.datacube import DataCube

API_URL = "https://oeo.test"

DEFAULT_S2_METADATA = {
    "cube:dimensions": {
        "x": {"type": "spatial"},
        "y": {"type": "spatial"},
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["B02", "B03", "B04", "B08"]}
    },
    "summaries": {
        "eo:bands": [
            {"name": "B02", "common_name": "blue", "center_wavelength": 0.4966},
            {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
            {"name": "B04", "common_name": "red", "center_wavelength": 0.6645},
            {"name": "B08", "common_name": "nir", "center_wavelength": 0.8351},
        ]
    },
}


def _setup_connection(api_version, requests_mock) -> Connection:
    # TODO: make this more reusable?
    requests_mock.get(API_URL + "/", json={"api_version": api_version})
    # Classic Sentinel2 collection
    requests_mock.get(API_URL + "/collections/SENTINEL2_RADIOMETRY_10M", json=DEFAULT_S2_METADATA)
    # Alias for quick tests
    requests_mock.get(API_URL + "/collections/S2", json=DEFAULT_S2_METADATA)
    # Some other collections
    setup_collection_metadata(requests_mock=requests_mock, cid="MASK", bands=["CLOUDS", "WATER"])
    setup_collection_metadata(requests_mock=requests_mock, cid="SENTINEL2_SCF", bands=["SCENECLASSIFICATION", "MSK"])

    requests_mock.get(API_URL + "/file_formats", json={
        "output": {
            "GTiff": {"gis_data_types": ["raster"]},
            "netCDF": {"gis_data_types": ["raster"]},
            "csv": {"gis_data_types": ["table"]},
        }
    })
    requests_mock.get(API_URL + "/udf_runtimes", json={
        "Python": {"type": "language", "default": "3", "versions": {"3": {"libraries": {}}}},
        "R": {"type": "language", "default": "4", "versions": {"4": {"libraries": {}}}},
    })

    return openeo.connect(API_URL)


def setup_collection_metadata(requests_mock, cid: str, bands: List[str]):
    """Set up mock collection metadata"""
    requests_mock.get(API_URL + f"/collections/{cid}", json={
        "cube:dimensions": {
            "bands": {"type": "bands", "values": bands}
        },
        "summaries": {
            "eo:bands": [{"name": b} for b in bands]
        },
    })


@pytest.fixture
def connection(api_version, requests_mock) -> Connection:
    """Connection fixture to a backend of given version with some image collections."""
    return _setup_connection(api_version, requests_mock)


@pytest.fixture
def con100(requests_mock) -> Connection:
    """Connection fixture to a 1.0.0 backend with some image collections."""
    return _setup_connection("1.0.0", requests_mock)


@pytest.fixture
def s2cube(connection, api_version) -> DataCube:
    return connection.load_collection("S2")

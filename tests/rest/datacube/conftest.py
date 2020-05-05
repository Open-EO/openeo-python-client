import pytest

import openeo
import openeo.internal.graphbuilder_040
from openeo.rest.connection import Connection
from openeo.rest.datacube import DataCube

API_URL = "https://oeo.net"


@pytest.fixture(params=["0.4.0", "1.0.0"])
def api_version(request):
    return request.param


def _setup_connection(api_version, requests_mock) -> Connection:
    # TODO: make this more reusable?
    requests_mock.get(API_URL + "/", json={"api_version": api_version})
    s2_properties = {
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
    # Classic Sentinel2 collection
    requests_mock.get(API_URL + "/collections/SENTINEL2_RADIOMETRY_10M", json=s2_properties)
    # Alias for quick tests
    requests_mock.get(API_URL + "/collections/S2", json=s2_properties)
    # Some other collections
    requests_mock.get(API_URL + "/collections/MASK", json={})
    requests_mock.get(API_URL + "/collections/SENTINEL2_SCF", json={
        "cube:dimensions": {
            "bands": {"type": "bands", "values": ["SCENECLASSIFICATION", "MSK"]}
        },
        "summaries": {
            "eo:bands": [
                {"name": "SCENECLASSIFICATION"},
                {"name": "MSK"},
            ]
        },
    })
    return openeo.connect(API_URL)


@pytest.fixture
def connection(api_version, requests_mock) -> Connection:
    """Connection fixture to a backend of given version with some image collections."""
    return _setup_connection(api_version, requests_mock)


@pytest.fixture
def con040(requests_mock) -> Connection:
    """Connection fixture to a 0.4.0 backend with some image collections."""
    return _setup_connection("0.4.0", requests_mock)


@pytest.fixture
def con100(requests_mock) -> Connection:
    """Connection fixture to a 1.0.0 backend with some image collections."""
    return _setup_connection("1.0.0", requests_mock)


@pytest.fixture
def s2cube(connection, api_version) -> DataCube:
    return connection.load_collection("S2")

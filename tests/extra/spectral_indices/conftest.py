import pytest

import openeo
from openeo.rest.connection import Connection
from openeo.util import dict_no_none

API_URL = "https://oeo.test"


def _setup_connection(api_version, requests_mock) -> Connection:
    requests_mock.get(API_URL + "/", json={"api_version": api_version})

    # Classic Sentinel2 collection
    sentinel2_bands = [
        ("B01", "coastal aerosol"),
        ("B02", "blue"),
        ("B03", "green"),
        ("B04", "red"),
        ("B05", "nir"),
        ("B06", None),
        ("B07", None),
        ("B08", "nir"),
        ("B8A", "nir08"),
        ("B09", "nir09"),
        ("B11", "swir16"),
        ("B12", "swir22"),
    ]
    requests_mock.get(
        API_URL + "/collections/SENTINEL2",
        json={
            "id": "SENTINEL2",
            "cube:dimensions": {
                "x": {"type": "spatial"},
                "y": {"type": "spatial"},
                "t": {"type": "temporal"},
                "bands": {"type": "bands", "values": [n for n, _ in sentinel2_bands]},
            },
            "summaries": {"eo:bands": [dict_no_none(name=n, common_name=c) for n, c in sentinel2_bands]},
        },
    )

    # TODO: add other collections: Landsat, Modis, ProbaV, ...

    return openeo.connect(API_URL)


@pytest.fixture
def con(requests_mock) -> Connection:
    """Connection fixture to a 1.0.0 backend with some image collections."""
    return _setup_connection("1.0.0", requests_mock)

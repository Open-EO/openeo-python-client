from typing import List, Tuple

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
    landsat8_bands = [
        ("B1", "coastal"),
        ("B2", "blue"),
        ("B3", "green"),
        ("B4", "red"),
        ("B5", "nir"),
        ("B6", "swir16"),
        ("B7", "swir22"),
    ]

    def collection_metadata(id: str, bands: List[Tuple[str, str]]) -> dict:
        """Builder for collection metadata construct"""
        return {
            "id": id,
            "cube:dimensions": {
                "x": {"type": "spatial"},
                "y": {"type": "spatial"},
                "t": {"type": "temporal"},
                "bands": {"type": "bands", "values": [n for n, _ in bands]},
            },
            "summaries": {"eo:bands": [dict_no_none(name=n, common_name=c) for n, c in bands]},
        }

    requests_mock.get(
        API_URL + "/collections/SENTINEL2",
        json=collection_metadata(id="SENTINEL2", bands=sentinel2_bands),
    )
    # SENTINEL2-like collections but with a different IDs
    requests_mock.get(
        API_URL + "/collections/FOO_SENTINEL2_L2A",
        json=collection_metadata(id="FOO_SENTINEL2_L2A", bands=sentinel2_bands),
    )
    requests_mock.get(
        API_URL + "/collections/NELITENS2",
        json=collection_metadata(id="NELITENS2", bands=sentinel2_bands),
    )
    # SENTINEL2-like collection with a different ID and custom band names
    requests_mock.get(
        API_URL + "/collections/ZENDIMEL2",
        json=collection_metadata(id="ZENDIMEL2", bands=[(f"Z2-{n}", c) for n, c in sentinel2_bands]),
    )

    requests_mock.get(
        API_URL + "/collections/LANDSAT8",
        json=collection_metadata(id="LANDSAT8", bands=landsat8_bands),
    )
    requests_mock.get(
        API_URL + "/collections/SANDLAT8",
        json=collection_metadata(id="SANDLAT8", bands=landsat8_bands),
    )
    requests_mock.get(
        API_URL + "/collections/ZANDLAD8",
        json=collection_metadata(id="ZANDLAD8", bands=[(f"Z8-{n}", c) for n, c in landsat8_bands]),
    )

    # TODO: add other collections: Landsat, Modis, ProbaV, ...

    return openeo.connect(API_URL)


@pytest.fixture
def con(requests_mock) -> Connection:
    """Connection fixture to a 1.0.0 backend with some image collections."""
    return _setup_connection("1.0.0", requests_mock)

"""

General cube method tests against both
- 1.0.0-style DataCube

"""

import contextlib
import json
import pathlib
from datetime import date, datetime
from unittest import mock

import dirty_equals
import numpy as np
import pytest
import requests
import shapely
import shapely.geometry

import openeo.processes
from openeo import collection_property
from openeo.api.process import Parameter
from openeo.internal.graph_building import PGNode
from openeo.metadata import SpatialDimension
from openeo.rest import BandMathException, OpenEoClientException
from openeo.rest._testing import build_capabilities
from openeo.rest.connection import Connection
from openeo.rest.datacube import DataCube
from openeo.rest.models.general import ValidationResponse
from openeo.testing.stac import StacDummyBuilder
from openeo.util import dict_no_none

from .. import get_download_graph
from .conftest import API_URL


def test_apply_dimension_temporal_cumsum(s2cube, api_version, test_data):
    cumsum = s2cube.apply_dimension('cumsum', dimension="t")
    actual_graph = get_download_graph(cumsum)
    expected_graph = test_data.load_json("{v}/apply_dimension_temporal_cumsum.json".format(v=api_version))
    assert actual_graph == expected_graph


def test_apply_dimension_invalid_dimension_with_metadata(s2cube):
    with pytest.raises(ValueError, match="Invalid dimension"):
        s2cube.apply_dimension("cumsum", dimension="olapola")


def test_apply_dimension_invalid_dimension_no_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.apply_dimension("cumsum", dimension="olapola")
    assert get_download_graph(cube)["applydimension1"] == {
        "process_id": "apply_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "olapola",
            "process": {
                "process_graph": {
                    "cumsum1": {
                        "arguments": {"data": {"from_parameter": "data"}},
                        "process_id": "cumsum",
                        "result": True,
                    }
                }
            },
        },
    }


def test_min_time(s2cube, api_version, test_data):
    min_time = s2cube.min_time()
    actual_graph = get_download_graph(min_time)
    expected_graph = test_data.load_json("{v}/min_time.json".format(v=api_version))
    assert actual_graph == expected_graph


def _get_leaf_node(cube, force_flat=True) -> dict:
    """Get leaf node (node with result=True), supporting old and new style of graph building."""
    # TODO: replace this with get_download_graph
    if isinstance(cube, DataCube):
        if force_flat:
            flat_graph = cube.flat_graph()
            node, = [n for n in flat_graph.values() if n.get("result")]
            return node
        else:
            return cube._pg.to_dict()
    else:
        raise ValueError(repr(cube))


class TestDataCube:

    def test_load_collection_connectionless_basic(self):
        cube = DataCube.load_collection("T3")
        assert cube.flat_graph() == {
            "loadcollection1": {
                "arguments": {"id": "T3", "spatial_extent": None, "temporal_extent": None},
                "process_id": "load_collection",
                "result": True,
            }
        }

    def test_load_collection_connectionless_full(self):
        cube = DataCube.load_collection(
            "T3",
            spatial_extent={"west": 1, "east": 2, "north": 3, "south": 4},
            temporal_extent=["2019-01-01", "2019-02-01"],
            bands=["RED", "GREEN"],
            properties=[collection_property("orbit") == "low"],
        )
        assert cube.flat_graph() == {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "T3",
                    "spatial_extent": {"east": 2, "north": 3, "south": 4, "west": 1},
                    "temporal_extent": ["2019-01-01", "2019-02-01"],
                    "bands": ["RED", "GREEN"],
                    "properties": {
                        "orbit": {
                            "process_graph": {
                                "eq1": {
                                    "process_id": "eq",
                                    "arguments": {"x": {"from_parameter": "value"}, "y": "low"},
                                    "result": True,
                                }
                            }
                        }
                    },
                },
                "result": True,
            }
        }

    def test_load_collection_connectionless_temporal_extent_shortcut(self):
        cube = DataCube.load_collection("T3", temporal_extent="2024-09")
        assert cube.flat_graph() == {
            "loadcollection1": {
                "arguments": {"id": "T3", "spatial_extent": None, "temporal_extent": ["2024-09-01", "2024-10-01"]},
                "process_id": "load_collection",
                "result": True,
            }
        }

    def test_load_collection_spatial_extent_bbox(self, dummy_backend):
        spatial_extent = {"west": 1, "south": 2, "east": 3, "north": 4}
        cube = DataCube.load_collection("S2", spatial_extent=spatial_extent, connection=dummy_backend.connection)
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadcollection1"]["arguments"] == {
            "id": "S2",
            "spatial_extent": {"west": 1, "south": 2, "east": 3, "north": 4},
            "temporal_extent": None,
        }

    def test_load_collection_spatial_extent_shapely(self, dummy_backend):
        polygon = shapely.geometry.Polygon([(3, 51), (4, 51), (4, 52), (3, 52)])
        cube = DataCube.load_collection("S2", spatial_extent=polygon, connection=dummy_backend.connection)
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadcollection1"]["arguments"] == {
            "id": "S2",
            "spatial_extent": {
                "type": "Polygon",
                "coordinates": [[[3, 51], [4, 51], [4, 52], [3, 52], [3, 51]]],
            },
            "temporal_extent": None,
        }

    @pytest.mark.parametrize("path_factory", [str, pathlib.Path])
    def test_load_collection_spatial_extent_local_path(self, dummy_backend, path_factory, test_data):
        path = path_factory(test_data.get_path("geojson/polygon02.json"))
        cube = DataCube.load_collection("S2", spatial_extent=path, connection=dummy_backend.connection)
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadcollection1"]["arguments"] == {
            "id": "S2",
            "spatial_extent": {"type": "Polygon", "coordinates": [[[3, 50], [4, 50], [4, 51], [3, 50]]]},
            "temporal_extent": None,
        }

    def test_load_collection_spatial_extent_url(self, dummy_backend):
        cube = DataCube.load_collection(
            "S2", spatial_extent="https://geo.test/geometry.json", connection=dummy_backend.connection
        )
        cube.execute()
        assert dummy_backend.get_sync_pg() == {
            "loadurl1": {
                "process_id": "load_url",
                "arguments": {"format": "GeoJSON", "url": "https://geo.test/geometry.json"},
            },
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2",
                    "spatial_extent": {"from_node": "loadurl1"},
                    "temporal_extent": None,
                },
                "result": True,
            },
        }

    def test_load_collection_spatial_extent_parameter(self, dummy_backend):
        cube = DataCube.load_collection(
            "S2", spatial_extent=Parameter.geojson("zpatial_extent"), connection=dummy_backend.connection
        )
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadcollection1"]["arguments"] == {
            "id": "S2",
            "spatial_extent": {"from_parameter": "zpatial_extent"},
            "temporal_extent": None,
        }

    def test_load_collection_connectionless_save_result(self):
        cube = DataCube.load_collection("T3").save_result(format="GTiff")
        assert cube.flat_graph() == {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "T3", "spatial_extent": None, "temporal_extent": None},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "format": "GTiff",
                    "options": {},
                },
                "result": True,
            },
        }

    def test_load_stac_connectionless_basic(self):
        cube = DataCube.load_stac("https://provider.test/dataset")
        assert cube.flat_graph() == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": "https://provider.test/dataset"},
                "result": True,
            }
        }

    def test_load_stac_connectionless_save_result(self):
        cube = DataCube.load_stac("https://provider.test/dataset").save_result(format="GTiff")
        assert cube.flat_graph() == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": "https://provider.test/dataset"},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadstac1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }

    def test_load_stac_spatial_extent_bbox(self, dummy_backend):
        spatial_extent = {"west": 1, "south": 2, "east": 3, "north": 4}
        cube = DataCube.load_stac(
            "https://stac.test/data", spatial_extent=spatial_extent, connection=dummy_backend.connection
        )
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadstac1"]["arguments"] == {
            "url": "https://stac.test/data",
            "spatial_extent": {"west": 1, "south": 2, "east": 3, "north": 4},
        }

    def test_load_stac_spatial_extent_shapely(self, dummy_backend):
        polygon = shapely.geometry.Polygon([(3, 51), (4, 51), (4, 52), (3, 52)])
        cube = DataCube.load_stac("https://stac.test/data", spatial_extent=polygon, connection=dummy_backend.connection)
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadstac1"]["arguments"] == {
            "url": "https://stac.test/data",
            "spatial_extent": {
                "type": "Polygon",
                "coordinates": [[[3, 51], [4, 51], [4, 52], [3, 52], [3, 51]]],
            },
        }

    @pytest.mark.parametrize("path_factory", [str, pathlib.Path])
    def test_load_stac_spatial_extent_local_path(self, dummy_backend, path_factory, test_data):
        path = path_factory(test_data.get_path("geojson/polygon02.json"))
        cube = DataCube.load_stac("https://stac.test/data", spatial_extent=path, connection=dummy_backend.connection)
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadstac1"]["arguments"] == {
            "url": "https://stac.test/data",
            "spatial_extent": {"type": "Polygon", "coordinates": [[[3, 50], [4, 50], [4, 51], [3, 50]]]},
        }

    def test_load_stac_spatial_extent_url(self, dummy_backend):
        cube = DataCube.load_stac(
            "https://stac.test/data",
            spatial_extent="https://geo.test/geometry.json",
            connection=dummy_backend.connection,
        )
        cube.execute()
        assert dummy_backend.get_sync_pg() == {
            "loadurl1": {
                "process_id": "load_url",
                "arguments": {"format": "GeoJSON", "url": "https://geo.test/geometry.json"},
            },
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {
                    "url": "https://stac.test/data",
                    "spatial_extent": {"from_node": "loadurl1"},
                },
                "result": True,
            },
        }

    def test_load_stac_spatial_extent_parameter(self, dummy_backend):
        spatial_extent = Parameter.geojson("zpatial_extent")
        cube = DataCube.load_stac(
            "https://stac.test/data", spatial_extent=spatial_extent, connection=dummy_backend.connection
        )
        cube.execute()
        assert dummy_backend.get_sync_pg()["loadstac1"]["arguments"] == {
            "url": "https://stac.test/data",
            "spatial_extent": {"from_parameter": "zpatial_extent"},
        }

    @pytest.mark.parametrize(
        "stac_metadata",
        [
            StacDummyBuilder.item(),
            StacDummyBuilder.collection(),
            StacDummyBuilder.catalog(),
        ],
    )
    def test_load_stac_resample_spatial(self, dummy_backend, tmp_path, stac_metadata):
        """https://github.com/Open-EO/openeo-python-client/issues/737"""
        stac_path = tmp_path / "stac.json"
        # TODO #738 real request mocking of STAC resources compatible with pystac?
        stac_path.write_text(json.dumps(stac_metadata))
        cube = dummy_backend.connection.load_stac(str(stac_path))
        cube = cube.resample_spatial(resolution=10, projection=4326)
        cube.execute()
        assert dummy_backend.get_sync_pg() == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": dirty_equals.IsStr(regex=r".*[\\/]stac.json")},
            },
            "resamplespatial1": {
                "process_id": "resample_spatial",
                "arguments": {
                    "data": {"from_node": "loadstac1"},
                    "projection": 4326,
                    "resolution": 10,
                    "method": "near",
                    "align": "upper-left",
                },
                "result": True,
            },
        }


def test_filter_temporal_basic_positional_args(s2cube):
    im = s2cube.filter_temporal("2016-01-01", "2016-03-10")
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_basic_start_end(s2cube):
    im = s2cube.filter_temporal(start_date="2016-01-01", end_date="2016-03-10")
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_basic_extent(s2cube):
    im = s2cube.filter_temporal(extent=("2016-01-01", "2016-03-10"))
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


@pytest.mark.parametrize(
    "args,kwargs,extent",
    [
        ((), {}, [None, None]),
        (("2016-01-01", "2016-03-10"), {}, ["2016-01-01", "2016-03-10"]),
        ((("2016-01-01", "2016-03-10"),), {}, ["2016-01-01", "2016-03-10"]),
        ((["2016-01-01", "2016-03-10"],), {}, ["2016-01-01", "2016-03-10"]),
        ((date(2016, 1, 1), date(2016, 3, 10)), {}, ["2016-01-01", "2016-03-10"]),
        (
            (datetime(2016, 1, 1, 12, 34), datetime(2016, 3, 10, 23, 45)),
            {},
            ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"],
        ),
        ((), {"start_date": "2016-01-01", "end_date": "2016-03-10"}, ["2016-01-01", "2016-03-10"]),
        ((), {"start_date": "2016-01-01"}, ["2016-01-01", None]),
        ((), {"end_date": "2016-03-10"}, [None, "2016-03-10"]),
        ((), {"start_date": date(2016, 1, 1), "end_date": date(2016, 3, 10)}, ["2016-01-01", "2016-03-10"]),
        (
            (),
            {"start_date": datetime(2016, 1, 1, 12, 34), "end_date": datetime(2016, 3, 10, 23, 45)},
            ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"],
        ),
        ((), {"extent": ("2016-01-01", "2016-03-10")}, ["2016-01-01", "2016-03-10"]),
        ((), {"extent": ("2016-01-01", None)}, ["2016-01-01", None]),
        ((), {"extent": (None, "2016-03-10")}, [None, "2016-03-10"]),
        ((), {"extent": (date(2016, 1, 1), date(2016, 3, 10))}, ["2016-01-01", "2016-03-10"]),
        (
            (),
            {"extent": (datetime(2016, 1, 1, 12, 34), datetime(2016, 3, 10, 23, 45))},
            ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"],
        ),
    ],
)
def test_filter_temporal_generic(s2cube, args, kwargs, extent):
    im = s2cube.filter_temporal(*args, **kwargs)
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == extent


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        (["2016", None], ["2016-01-01", None]),
        (["2016-03", None], ["2016-03-01", None]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-03", "2016-04"], ["2016-03-01", "2016-04-01"]),
        (["2016-03", "2018-12"], ["2016-03-01", "2018-12-01"]),
        (["2016", "2016-01"], ["2016-01-01", "2016-01-01"]),
        (["2016", "2016-04"], ["2016-01-01", "2016-04-01"]),
        ([None, "2016-04"], [None, "2016-04-01"]),
    ],
)
def test_filter_temporal_extent_tuple_with_shorthand_dates(s2cube: DataCube, extent, expected):
    """Verify it supports shorthand dates that represent years or months."""
    im = s2cube.filter_temporal(extent=extent)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_temporal"
    assert graph["arguments"]["extent"] == expected


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        ("2016", ["2016-01-01", "2017-01-01"]),
        ("2016-01", ["2016-01-01", "2016-02-01"]),
        ("2016-04", ["2016-04-01", "2016-05-01"]),
        ("2016-12", ["2016-12-01", "2017-01-01"]),
        ("2016-04-11", ["2016-12-01", "2017-01-01"]),
    ],
)
def test_filter_temporal_extent_single_date_string(s2cube: DataCube, extent, expected):
    """Verify it supports single string extent."""
    im = s2cube.filter_temporal(extent=extent)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_temporal"
    assert graph["arguments"]["extent"] == expected


@pytest.mark.parametrize(
    "extent,expected",
    [
        (("2016-01-01", None), ["2016-01-01", None]),
        (("2016-01-01", "2016-03-10"), ["2016-01-01", "2016-03-10"]),
        ((None, "2016-03-10"), [None, "2016-03-10"]),
        (["2016-01-01", None], ["2016-01-01", None]),
    ],
)
def test_load_collection_filter_temporal(connection, api_version, extent, expected):
    cube: DataCube = connection.load_collection("S2", temporal_extent=extent)
    flat_graph = cube.flat_graph()
    assert flat_graph["loadcollection1"]["arguments"]["temporal_extent"] == expected


@pytest.mark.parametrize(
    "extent,expected",
    [
        # Test that the simplest/shortest syntax works: temporal_extent="2016"
        ("2016", ["2016-01-01", "2017-01-01"]),
        ("2016-02", ["2016-02-01", "2016-03-01"]),
        ("2016-02-03", ["2016-02-03", "2016-02-04"]),
        # Test date abbreviations using tuples for the extent
        (["2016", None], ["2016-01-01", None]),
        (["2016-02", None], ["2016-02-01", None]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-02", "2016-08"], ["2016-02-01", "2016-08-01"]),
        ([None, "2016"], [None, "2016-01-01"]),
        ([None, "2016-02"], [None, "2016-02-01"]),
    ],
)
def test_load_collection_temporal_extent_with_shorthand_date_strings(connection, api_version, extent, expected):
    """Verify it supports abbreviated date strings."""
    cube: DataCube = connection.load_collection("S2", temporal_extent=extent)
    flat_graph = cube.flat_graph()
    assert flat_graph["loadcollection1"]["arguments"]["temporal_extent"] == expected


def test_load_collection_bands_name(connection, api_version, test_data):
    im = connection.load_collection("S2", bands=["B08", "B04"])
    expected = test_data.load_json("{v}/load_collection_bands.json".format(v=api_version))
    assert im.flat_graph() == expected


def test_load_collection_bands_single_band(connection, api_version, test_data):
    im = connection.load_collection("S2", bands="B08")
    expected = test_data.load_json("{v}/load_collection_bands.json".format(v=api_version))
    expected["loadcollection1"]["arguments"]["bands"] = ["B08"]
    assert im.flat_graph() == expected


def test_load_collection_bands_common_name(connection, api_version, test_data):
    im = connection.load_collection("S2", bands=["nir", "red"])
    expected = test_data.load_json("{v}/load_collection_bands.json".format(v=api_version))
    expected["loadcollection1"]["arguments"]["bands"] = ["nir", "red"]
    assert im.flat_graph() == expected


def test_load_collection_bands_band_index(connection, api_version, test_data):
    im = connection.load_collection("S2", bands=[3, 2])
    expected = test_data.load_json("{v}/load_collection_bands.json".format(v=api_version))
    assert im.flat_graph() == expected


def test_load_collection_bands_and_band_math(connection, api_version, test_data):
    cube = connection.load_collection("S2", bands=["B03", "B04"])
    b4 = cube.band("B04")
    b3 = cube.band("B03")
    x = b4 - b3
    expected = test_data.load_json("{v}/load_collection_bands_and_band_math.json".format(v=api_version))
    assert x.flat_graph() == expected


def test_filter_bands_name(s2cube, api_version, test_data):
    im = s2cube.filter_bands(["B08", "B04"])
    expected = test_data.load_json("{v}/filter_bands.json".format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08", "B04"]
    assert im.flat_graph() == expected


def test_filter_bands_single_band(s2cube, api_version, test_data):
    im = s2cube.filter_bands("B08")
    expected = test_data.load_json("{v}/filter_bands.json".format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08"]
    assert im.flat_graph() == expected


def test_filter_bands_common_name(s2cube, api_version, test_data):
    im = s2cube.filter_bands(["nir", "red"])
    expected = test_data.load_json("{v}/filter_bands.json".format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["nir", "red"]
    assert im.flat_graph() == expected


def test_filter_bands_index(s2cube, api_version, test_data):
    im = s2cube.filter_bands([3, 2])
    expected = test_data.load_json("{v}/filter_bands.json".format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08", "B04"]
    assert im.flat_graph() == expected


def test_filter_bands_invalid_bands_with_metadata(s2cube):
    with pytest.raises(ValueError, match="Invalid band name/index 'apple'"):
        _ = s2cube.filter_bands(["apple", "banana"])


def test_filter_bands_invalid_bands_without_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.filter_bands(["apple", "banana"])
    assert get_download_graph(cube)["filterbands1"] == {
        "process_id": "filter_bands",
        "arguments": {"data": {"from_node": "loadcollection1"}, "bands": ["apple", "banana"]},
    }
    cube = cube.filter_bands(["banana"])
    assert get_download_graph(cube)["filterbands2"] == {
        "process_id": "filter_bands",
        "arguments": {"data": {"from_node": "filterbands1"}, "bands": ["banana"]},
    }


def test_filter_bbox_minimal(s2cube):
    im = s2cube.filter_bbox(west=3.0, east=3.1, north=51.1, south=51.0)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {"west": 3.0, "east": 3.1, "north": 51.1, "south": 51.0}


def test_filter_bbox_crs_4326(s2cube):
    im = s2cube.filter_bbox(west=3.0, east=3.1, north=51.1, south=51.0, crs=4326)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {"west": 3.0, "east": 3.1, "north": 51.1, "south": 51.0, "crs": 4326}


def test_filter_bbox_crs_32632(s2cube):
    im = s2cube.filter_bbox(
        west=652000, east=672000, north=5161000, south=5181000, crs=32632
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": 32632
    }


def test_filter_bbox_base_height(s2cube):
    im = s2cube.filter_bbox(
        west=652000, east=672000, north=5161000, south=5181000, crs=32632,
        base=100, height=200,
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": 32632,
        "base": 100, "height": 200,
    }


@pytest.mark.parametrize(["kwargs", "expected"], [
    ({}, {}),
    ({"crs": None}, {}),
    ({"crs": 4326}, {"crs": 4326}),
    ({"crs": 32632}, {"crs": 32632}),
    ({"base": None}, {}),
    ({"base": 123}, {"base": 123}),
    ({"height": None}, {}),
    ({"height": 456}, {"height": 456}),
    ({"base": None, "height": 456}, {"height": 456}),
    ({"base": 123, "height": 456}, {"base": 123, "height": 456}),
])
def test_filter_bbox_default_handling(s2cube, kwargs, expected):
    im = s2cube.filter_bbox(west=3, east=4, south=8, north=9, **kwargs)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == dict(west=3, east=4, south=8, north=9, **expected)


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        # test regular extents
        (("2016-01-01", None), ["2016-01-01", None]),
        (("2016-01-01", "2016-03-10"), ["2016-01-01", "2016-03-10"]),
        ((None, "2016-03-10"), [None, "2016-03-10"]),
        (["2016-01-01", None], ["2016-01-01", None]),
        # test the date abbreviations
        (["2016", None], ["2016-01-01", None]),
        (["2016-02", None], ["2016-02-01", None]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-02", "2016-02"], ["2016-02-01", "2016-02-01"]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-02", "2016-08"], ["2016-02-01", "2016-08-01"]),
        ([None, "2016"], [None, "2016-01-01"]),
        ([None, "2016-02"], [None, "2016-02-01"]),
    ],
)
def test_filter_temporal_general(s2cube: DataCube, api_version, extent, expected):
    # First test it via positional args
    cube_pos_args: DataCube = s2cube.filter_temporal(extent[0], extent[1])
    flat_graph_pos_args = cube_pos_args.flat_graph()
    assert flat_graph_pos_args["filtertemporal1"]["arguments"]["extent"] == expected

    # Using start_date and end_date should give identical result
    cube_start_end: DataCube = s2cube.filter_temporal(start_date=extent[0], end_date=extent[1])
    flat_graph_start_end = cube_start_end.flat_graph()
    assert flat_graph_start_end["filtertemporal1"]["arguments"]["extent"] == expected

    # And using the extent parameter should also do exactly the same.
    cube_extent: DataCube = s2cube.filter_temporal(extent=extent)
    flat_graph_extent = cube_extent.flat_graph()
    assert flat_graph_extent["filtertemporal1"]["arguments"]["extent"] == expected


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        ("2016", ["2016-01-01", "2017-01-01"]),
        ("2016-02", ["2016-02-01", "2016-03-01"]),
        ("2016-12", ["2016-12-01", "2017-01-01"]),
        ("2016-03-04", ["2016-03-04", "2016-03-05"]),
    ],
)
def test_filter_temporal_extent_single_date_string(s2cube: DataCube, api_version, extent, expected):
    cube_extent: DataCube = s2cube.filter_temporal(extent=extent)
    flat_graph_extent = cube_extent.flat_graph()
    assert flat_graph_extent["filtertemporal1"]["arguments"]["extent"] == expected


@pytest.mark.parametrize(
    ["arg", "expect_failure"],
    [
        ("2024-09-24", True),
        ("2024-09", True),
        ("2024", True),
        (("2024-09-01", "2024-09-10"), False),
        (["2024-09-01", "2024-09-10"], False),
        (Parameter.temporal_interval("window"), False),
    ],
)
def test_filter_temporal_single_arg(s2cube: DataCube, arg, expect_failure):
    if expect_failure:
        context = pytest.raises(
            OpenEoClientException, match="filter_temporal.*with a single string argument.*is ambiguous.*"
        )
    else:
        context = contextlib.nullcontext()
    with context:
        _ = s2cube.filter_temporal(arg)


@pytest.mark.parametrize(
    "udf_factory",
    [
        (lambda data, udf, runtime: openeo.processes.run_udf(data=data, udf=udf, runtime=runtime)),
        (lambda data, udf, runtime: PGNode(process_id="run_udf", data=data, udf=udf, runtime=runtime)),
    ],
)
def test_filter_temporal_from_udf(s2cube: DataCube, udf_factory):
    temporal_extent = udf_factory(data=[1, 2, 3], udf="print('hello time')", runtime="Python")
    cube = s2cube.filter_temporal(temporal_extent)
    assert get_download_graph(cube, drop_save_result=True) == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "runudf1": {
            "process_id": "run_udf",
            "arguments": {"data": [1, 2, 3], "udf": "print('hello time')", "runtime": "Python"},
        },
        "filtertemporal1": {
            "process_id": "filter_temporal",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "extent": {"from_node": "runudf1"},
            },
        },
    }


@pytest.mark.parametrize(
    "udf_factory",
    [
        (lambda data, udf, runtime: openeo.processes.run_udf(data=data, udf=udf, runtime=runtime)),
        (lambda data, udf, runtime: PGNode(process_id="run_udf", data=data, udf=udf, runtime=runtime)),
    ],
)
def test_filter_temporal_start_end_from_udf(s2cube: DataCube, udf_factory):
    start = udf_factory(data=[1, 2, 3], udf="print('hello start')", runtime="Python")
    end = udf_factory(data=[4, 5, 6], udf="print('hello end')", runtime="Python")
    cube = s2cube.filter_temporal(start_date=start, end_date=end)
    assert get_download_graph(cube, drop_save_result=True) == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "runudf1": {
            "process_id": "run_udf",
            "arguments": {"data": [1, 2, 3], "udf": "print('hello start')", "runtime": "Python"},
        },
        "runudf2": {
            "process_id": "run_udf",
            "arguments": {"data": [4, 5, 6], "udf": "print('hello end')", "runtime": "Python"},
        },
        "filtertemporal1": {
            "process_id": "filter_temporal",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "extent": [{"from_node": "runudf1"}, {"from_node": "runudf2"}],
            },
        },
    }


def test_max_time(s2cube, api_version):
    im = s2cube.max_time()
    graph = _get_leaf_node(im, force_flat=True)
    assert graph["process_id"] == "reduce_dimension"
    assert graph["arguments"]["data"] == {"from_node": "loadcollection1"}
    assert graph["arguments"]["dimension"] == "t"
    callback = graph["arguments"]["reducer"]["process_graph"]["max1"]
    assert callback == {"arguments": {"data": {"from_parameter": "data"}}, "process_id": "max", "result": True}


def test_reduce_temporal_udf(s2cube, api_version):
    im = s2cube.reduce_temporal_udf("my custom code")
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "reduce_dimension"
    assert "data" in graph["arguments"]
    assert graph["arguments"]["dimension"] == "t"


def test_ndvi(s2cube, api_version):
    im = s2cube.ndvi()
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "ndvi"
    assert graph["arguments"] == {"data": {"from_node": "loadcollection1"}}


def test_mask_polygon(s2cube, api_version):
    polygon = shapely.geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
    expected_proces_id = "mask_polygon"
    im = s2cube.mask_polygon(mask=polygon)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == expected_proces_id
    assert graph["arguments"] == {
        "data": {'from_node': 'loadcollection1'},
        "mask": {
            'type': 'Polygon',
            'coordinates': (((0.0, 0.0), (1.9, 0.0), (1.9, 1.9), (0.0, 1.9), (0.0, 0.0)),),
        }
    }


def test_mask_raster(s2cube, connection, api_version):
    mask = connection.load_collection("MASK")
    im = s2cube.mask(mask=mask, replacement=102)
    graph = _get_leaf_node(im)
    assert graph == {
        "process_id": "mask",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "loadcollection2"},
            "replacement": 102,
        },
        "result": True,
    }


def test_apply_kernel(s2cube):
    kernel = [[0, 1, 0], [1, 1, 1], [0, 1, 0]]
    im = s2cube.apply_kernel(np.asarray(kernel), 3)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "apply_kernel"
    assert graph["arguments"] == {
        'data': {'from_node': 'loadcollection1'},
        'factor': 3,
        'border': 0,
        'replace_invalid': 0,
        'kernel': [[0, 1, 0], [1, 1, 1], [0, 1, 0]]
    }


def test_resample_spatial(s2cube):
    cube = s2cube.resample_spatial(resolution=[2.0, 3.0], projection=4578)
    assert get_download_graph(cube, drop_load_collection=True, drop_save_result=True) == {
        "resamplespatial1": {
            "process_id": "resample_spatial",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "resolution": [2.0, 3.0],
                "projection": 4578,
                "method": "near",
                "align": "upper-left",
            },
        }
    }

    assert cube.metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=None, crs=4578, step=2.0),
        SpatialDimension(name="y", extent=None, crs=4578, step=3.0),
    ]


def test_resample_spatial_no_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.resample_spatial(resolution=(3, 5), projection=4578)
    assert get_download_graph(cube, drop_load_collection=True, drop_save_result=True) == {
        "resamplespatial1": {
            "process_id": "resample_spatial",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "resolution": [3, 5],
                "projection": 4578,
                "method": "near",
                "align": "upper-left",
            },
        }
    }
    assert cube.metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=[None, None], crs=4578, step=3.0),
        SpatialDimension(name="y", extent=[None, None], crs=4578, step=5.0),
    ]


def test_resample_cube_spatial(s2cube):
    cube1 = s2cube.resample_spatial(resolution=[2.0, 3.0], projection=4578)
    cube2 = s2cube.resample_spatial(resolution=10, projection=32631)

    cube12 = cube1.resample_cube_spatial(target=cube2)
    assert get_download_graph(cube12, drop_load_collection=True, drop_save_result=True) == {
        "resamplespatial1": {
            "process_id": "resample_spatial",
            "arguments": {
                "align": "upper-left",
                "data": {"from_node": "loadcollection1"},
                "method": "near",
                "projection": 4578,
                "resolution": [2.0, 3.0],
            },
        },
        "resamplespatial2": {
            "process_id": "resample_spatial",
            "arguments": {
                "align": "upper-left",
                "data": {"from_node": "loadcollection1"},
                "method": "near",
                "projection": 32631,
                "resolution": 10,
            },
        },
        "resamplecubespatial1": {
            "arguments": {
                "data": {"from_node": "resamplespatial1"},
                "method": "near",
                "target": {"from_node": "resamplespatial2"},
            },
            "process_id": "resample_cube_spatial",
        },
    }
    assert cube12.metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=None, crs=32631, step=10),
        SpatialDimension(name="y", extent=None, crs=32631, step=10),
    ]

    cube21 = cube2.resample_cube_spatial(target=cube1)
    assert get_download_graph(cube21, drop_load_collection=True, drop_save_result=True) == {
        "resamplespatial1": {
            "process_id": "resample_spatial",
            "arguments": {
                "align": "upper-left",
                "data": {"from_node": "loadcollection1"},
                "method": "near",
                "projection": 32631,
                "resolution": 10,
            },
        },
        "resamplespatial2": {
            "process_id": "resample_spatial",
            "arguments": {
                "align": "upper-left",
                "data": {"from_node": "loadcollection1"},
                "method": "near",
                "projection": 4578,
                "resolution": [2.0, 3.0],
            },
        },
        "resamplecubespatial1": {
            "arguments": {
                "data": {"from_node": "resamplespatial1"},
                "method": "near",
                "target": {"from_node": "resamplespatial2"},
            },
            "process_id": "resample_cube_spatial",
        },
    }
    assert cube21.metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=None, crs=4578, step=2.0),
        SpatialDimension(name="y", extent=None, crs=4578, step=3.0),
    ]


def test_resample_cube_spatial_no_source_metadata(s2cube, s2cube_without_metadata):
    cube = s2cube_without_metadata
    target = s2cube.resample_spatial(resolution=10, projection=32631)
    assert cube.metadata is None
    assert target.metadata is not None

    result = cube.resample_cube_spatial(target=target)
    assert result.metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=None, crs=32631, step=10),
        SpatialDimension(name="y", extent=None, crs=32631, step=10),
    ]


def test_resample_cube_spatial_preserve_non_spatial(s2cube):
    cube_xytb = s2cube.resample_spatial(resolution=11, projection=32631)
    cube_xyt = s2cube.reduce_dimension(dimension="bands", reducer="mean").resample_spatial(
        resolution=22, projection=32631
    )
    cube_xyb = s2cube.reduce_dimension(dimension="t", reducer="mean").resample_spatial(resolution=33, projection=32631)

    result = cube_xyt.resample_cube_spatial(target=cube_xytb)
    assert result.metadata.dimension_names() == ["t", "x", "y"]
    assert result.metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=None, crs=32631, step=11),
        SpatialDimension(name="y", extent=None, crs=32631, step=11),
    ]

    result = cube_xyb.resample_cube_spatial(target=cube_xyt)
    assert result.metadata.dimension_names() == ["bands", "x", "y"]
    assert result.metadata.spatial_dimensions == [
        SpatialDimension(name="x", extent=None, crs=32631, step=22),
        SpatialDimension(name="y", extent=None, crs=32631, step=22),
    ]


def test_resample_cube_spatial_no_target_metadata(s2cube, s2cube_without_metadata):
    cube = s2cube.resample_spatial(resolution=10, projection=32631)
    target = s2cube_without_metadata
    assert cube.metadata is not None
    assert target.metadata is None

    result = cube.resample_cube_spatial(target=target)
    assert result.metadata is None


def test_merge(s2cube, api_version, test_data):
    merged = s2cube.ndvi().merge(s2cube)
    expected_graph = test_data.load_json("{v}/merge_ndvi_self.json".format(v=api_version))
    assert merged.flat_graph() == expected_graph


def test_apply_absolute_str(s2cube, api_version, test_data):
    result = s2cube.apply("absolute")
    expected_graph = test_data.load_json("{v}/apply_absolute.json".format(v=api_version))
    assert result.flat_graph() == expected_graph


def test_subtract_dates_ep3129(s2cube, api_version):
    """EP-3129: band math between cubes of different time stamps is not supported (yet?)"""
    bbox = {"west": 5.16, "south": 51.23, "east": 5.18, "north": 51.25}
    date1 = "2018-08-01"
    date2 = "2019-10-28"
    im1 = s2cube.filter_temporal(date1, date1).filter_bbox(**bbox).band('B04')
    im2 = s2cube.filter_temporal(date2, date2).filter_bbox(**bbox).band('B04')

    with pytest.raises(BandMathException, match="between bands of different"):
        im2.subtract(im1)


def test_tiled_viewing_service(s2cube, connection, requests_mock, api_version, test_data):
    expected_graph = test_data.load_json("{v}/tiled_viewing_service.json".format(v=api_version))

    def check_request(request):
        assert request.json() == expected_graph
        return True

    requests_mock.post(
        API_URL + "/services",
        status_code=201,
        text='',
        headers={'Location': API_URL + "/services/sf00", 'OpenEO-Identifier': 'sf00'},
        additional_matcher=check_request
    )

    res = s2cube.tiled_viewing_service(type="WMTS", title="S2 Foo", description="Nice!", custom_param=45)
    assert res.service_id == 'sf00'


def test_apply_dimension(connection, requests_mock):
    requests_mock.get(API_URL + "/collections/S22", json={
        "cube:dimensions": {
            "color": {"type": "bands", "values": ["cyan", "magenta", "yellow", "black"]},
            "alpha": {"type": "spatial"},
            "date": {"type": "temporal"}
        }
    })
    s22 = connection.load_collection("S22")

    for dim in ["color", "alpha", "date"]:
        cube = s22.apply_dimension(dimension=dim, code="subtract_mean")
        assert cube.flat_graph()["applydimension1"]["process_id"] == "apply_dimension"
        assert cube.flat_graph()["applydimension1"]["arguments"]["dimension"] == dim
    with pytest.raises(ValueError, match="Invalid dimension 'wut'"):
        s22.apply_dimension(dimension='wut', code="subtract_mean")


def test_apply_dimension_unspecified_dimension(s2cube, dummy_backend, recwarn):
    s2cube.apply_dimension("cumsum").create_job()
    assert dummy_backend.get_pg(process_id="apply_dimension") == {
        "process_id": "apply_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "t",
            "process": {
                "process_graph": {
                    "cumsum1": {
                        "arguments": {"data": {"from_parameter": "data"}},
                        "process_id": "cumsum",
                        "result": True,
                    }
                }
            },
        },
    }
    assert str(recwarn.pop()) == dirty_equals.IsStr(
        regex=r".*UserDeprecationWarning.*apply_dimension.*without explicit.*dimension.*argument.*is deprecated.*filename.*[\\/]test_datacube.py.*"
    )


def test_download_path_str(connection, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + '/result', content=b"tiffdata")
    path = tmp_path / "tmp.tiff"
    connection.load_collection("S2").download(str(path), format="GTiff")
    assert path.read_bytes() == b"tiffdata"


def test_download_pathlib(connection, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + '/result', content=b"tiffdata")
    path = tmp_path / "tmp.tiff"
    connection.load_collection("S2").download(pathlib.Path(str(path)), format="GTIFF")
    assert path.read_bytes() == b"tiffdata"


def test_execute_json_decode(connection, requests_mock):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + "/result", content=b'{"foo": "bar"}')
    result = connection.load_collection("S2").execute(auto_decode=True)
    assert result == {"foo": "bar"}


def test_execute_decode_error(connection, requests_mock):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + "/result", content=b"tiffdata")
    with pytest.raises(OpenEoClientException, match="Failed to decode response as JSON.*$"):
        connection.load_collection("S2").execute(auto_decode=True)


def test_execute_json_raw(connection, requests_mock):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + "/result", content=b'{"foo": "bar"}')
    result = connection.load_collection("S2").execute(auto_decode=False)
    assert isinstance(result, requests.Response)
    assert result.content == b'{"foo": "bar"}'


def test_execute_tiff_raw(connection, requests_mock):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + "/result", content=b"tiffdata")
    result = connection.load_collection("S2").execute(auto_decode=False)
    assert isinstance(result, requests.Response)
    assert result.content == b"tiffdata"

@pytest.mark.parametrize(["filename", "expected_format"], [
    ("result.tiff", "GTiff"),
    ("result.tif", "GTiff"),
    ("result.gtiff", "GTiff"),
    ("result.geotiff", "GTiff"),
    ("result.nc", "netCDF"),
    ("result.netcdf", "netCDF"),
    ("result.csv", "CSV"),
])
@pytest.mark.parametrize("path_type", [str, pathlib.Path])
def test_download_format_guessing(
        connection, requests_mock, tmp_path, api_version, filename, path_type, expected_format
):
    requests_mock.get(API_URL + "/collections/S2", json={})

    def result_callback(request, context):
        post_data = request.json()
        pg = post_data["process"]["process_graph"]
        assert pg["saveresult1"]["arguments"]["format"] == expected_format
        return b"data"

    requests_mock.post(API_URL + '/result', content=result_callback)
    path = tmp_path / filename
    connection.load_collection("S2").download(path_type(path))
    assert path.read_bytes() == b"data"


@pytest.mark.parametrize(["format", "expected_format"], [
    ("GTiff", "GTiff"),
    ("netCDF", "netCDF"),
    (None, "GTiff"),
])
def test_download_bytes(connection, requests_mock, api_version, format, expected_format):
    requests_mock.get(API_URL + "/collections/S2", json={})

    def result_callback(request, context):
        post_data = request.json()
        pg = post_data["process"]["process_graph"]
        assert pg["saveresult1"]["arguments"]["format"] == expected_format
        return b"data"

    requests_mock.post(API_URL + '/result', content=result_callback)
    result = connection.load_collection("S2").download(format=format)
    assert result == b"data"


class TestExecuteBatch:
    @pytest.fixture
    def get_create_job_pg(self, connection):
        """Fixture to help intercepting the process graph that was passed to Connection.create_job"""
        with mock.patch.object(connection, "create_job") as create_job:

            def get() -> dict:
                assert create_job.call_count == 1
                return create_job.call_args[1]["process_graph"]

            yield get

    def test_create_job_defaults(self, s2cube, get_create_job_pg, recwarn, caplog):
        s2cube.create_job()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {},
            },
            "result": True,
        }
        assert recwarn.list == []
        assert caplog.records == []

    @pytest.mark.parametrize(
        ["out_format", "expected"],
        [("GTiff", "GTiff"), ("NetCDF", "NetCDF")],
    )
    def test_create_job_out_format(
        self, s2cube, get_create_job_pg, out_format, expected
    ):
        s2cube.create_job(out_format=out_format)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format", "expected"],
        [
            (None, None, "GTiff"),
            (None, "GTiff", "GTiff"),
            ("GTiff", None, "GTiff"),
            (None, "NetCDF", "NetCDF"),
            ("NetCDF", None, "NetCDF"),
        ],
    )
    def test_save_result_and_create_job_at_most_one_with_format(
        self,
        s2cube,
        get_create_job_pg,
        save_result_format,
        execute_format,
        expected,
    ):
        if save_result_format:
            res = s2cube.save_result(format=save_result_format)
        else:
            res = s2cube
        if execute_format:
            res.create_job(out_format=execute_format)
        else:
            res.create_job()

        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format"],
        [
            ("NetCDF", "NetCDF"),
            ("GTiff", "NetCDF"),
        ],
    )
    def test_save_result_and_create_job_both_with_format(self, s2cube, save_result_format, execute_format):
        cube = s2cube.save_result(format=save_result_format)
        with pytest.raises(TypeError, match="got an unexpected keyword argument 'out_format'"):
            cube.create_job(out_format=execute_format)

    @pytest.mark.parametrize(
        ["auto_add_save_result", "process_ids"],
        [
            (True, {"load_collection", "save_result"}),
            (False, {"load_collection"}),
        ],
    )
    def test_create_job_auto_add_save_result(self, s2cube, dummy_backend, auto_add_save_result, process_ids):
        s2cube.create_job(auto_add_save_result=auto_add_save_result)
        assert set(n["process_id"] for n in dummy_backend.get_pg().values()) == process_ids

    @pytest.mark.parametrize(
        ["create_kwargs", "expected"],
        [
            ({}, {}),
            ({"log_level": None}, {}),
            ({"log_level": "error"}, {"log_level": "error"}),
        ],
    )
    def test_create_job_log_level(self, s2cube, dummy_backend, create_kwargs, expected):
        s2cube.create_job(**create_kwargs)
        assert dummy_backend.get_batch_post_data() == {
            "process": {"process_graph": dirty_equals.IsPartialDict()},
            **expected,
        }

    def test_execute_batch_defaults(self, s2cube, get_create_job_pg, recwarn, caplog):
        s2cube.execute_batch()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {},
            },
            "result": True,
        }
        assert recwarn.list == []
        assert caplog.records == []

    @pytest.mark.parametrize(
        ["out_format", "expected"],
        [("GTiff", "GTiff"), ("NetCDF", "NetCDF")],
    )
    def test_execute_batch_out_format(
        self, s2cube, get_create_job_pg, out_format, expected
    ):
        s2cube.execute_batch(out_format=out_format)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["output_file", "expected"],
        [("cube.tiff", "GTiff"), ("cube.nc", "netCDF")],
    )
    def test_execute_batch_out_format_from_output_file(
        self, s2cube, get_create_job_pg, output_file, expected
    ):
        s2cube.execute_batch(outputfile=output_file)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format", "expected"],
        [
            (None, None, "GTiff"),
            (None, "GTiff", "GTiff"),
            ("GTiff", None, "GTiff"),
            (None, "NetCDF", "NetCDF"),
            ("NetCDF", None, "NetCDF"),
        ],
    )
    def test_save_result_and_execute_batch_at_most_one_with_format(
        self,
        s2cube,
        get_create_job_pg,
        save_result_format,
        execute_format,
        expected,
    ):
        if save_result_format:
            res = s2cube.save_result(format=save_result_format)
        else:
            res = s2cube
        if execute_format:
            res.execute_batch(out_format=execute_format)
        else:
            res.execute_batch()

        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format"],
        [
            ("NetCDF", "NetCDF"),
            ("GTiff", "NetCDF"),
        ],
    )
    def test_execute_batch_existing_save_result_incompatible(self, s2cube, save_result_format, execute_format):
        cube = s2cube.save_result(format=save_result_format)
        with pytest.raises(TypeError, match="got an unexpected keyword argument 'out_format'"):
            cube.execute_batch(out_format=execute_format)

    @pytest.mark.parametrize(
        ["save_result_format", "execute_output_file", "expected"],
        [
            (None, None, "GTiff"),
            (None, "result.tiff", "GTiff"),
            ("GTiff", None, "GTiff"),
            ("GTiff", "result.csv", "GTiff"),
            (None, "result.nc", "netCDF"),
            ("NetCDF", None, "NetCDF"),
            ("NetCDF", "result.csv", "NetCDF"),
            (None, "result.csv", "CSV"),
        ],
    )
    def test_save_result_and_execute_batch_weak_format(
        self,
        s2cube,
        get_create_job_pg,
        save_result_format,
        execute_output_file,
        expected,
    ):
        cube = s2cube
        if save_result_format:
            cube = cube.save_result(format=save_result_format)
        cube.execute_batch(outputfile=execute_output_file)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    def test_save_result_format_options_vs_create_job(elf, s2cube, get_create_job_pg):
        """https://github.com/Open-EO/openeo-python-client/issues/433"""
        cube = s2cube.save_result(format="GTiff", options={"filename_prefix": "wwt-2023-02"})
        _ = cube.create_job()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {"filename_prefix": "wwt-2023-02"},
            },
            "result": True,
        }

    def test_save_result_format_options_vs_execute_batch(elf, s2cube, get_create_job_pg):
        """https://github.com/Open-EO/openeo-python-client/issues/433"""
        cube = s2cube.save_result(format="GTiff", options={"filename_prefix": "wwt-2023-02"})
        _ = cube.execute_batch()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {"filename_prefix": "wwt-2023-02"},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["auto_add_save_result", "process_ids"],
        [
            (True, {"load_collection", "save_result"}),
            (False, {"load_collection"}),
        ],
    )
    def test_execute_batch_auto_add_save_result(self, s2cube, dummy_backend, auto_add_save_result, process_ids):
        s2cube.execute_batch(auto_add_save_result=auto_add_save_result)
        assert set(n["process_id"] for n in dummy_backend.get_pg().values()) == process_ids


class TestDataCubeValidation:
    """
    Test (auto) validation of datacube execution with `download`, `execute`, ...
    """

    _PG_S2 = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
            "result": True,
        },
    }
    _PG_S2_SAVE = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
            "result": True,
        },
    }

    @pytest.fixture(params=[False, True])
    def auto_validate(self, request) -> bool:
        """Fixture to parametrize auto_validate setting."""
        return request.param

    @pytest.fixture
    def connection(self, api_version, requests_mock, api_capabilities, auto_validate) -> Connection:
        requests_mock.get(API_URL, json=build_capabilities(api_version=api_version, **api_capabilities))
        con = Connection(API_URL, **dict_no_none(auto_validate=auto_validate))
        return con

    @pytest.fixture(autouse=True)
    def dummy_backend_setup(self, dummy_backend):
        dummy_backend.next_validation_errors = [{"code": "NoAdd", "message": "Don't add numbers"}]

    # Reusable list of (fixture) parameterization
    # of ["api_capabilities", "auto_validate", "validate", "validation_expected"]
    _VALIDATION_PARAMETER_SETS = [
        # No validation supported by backend: don't attempt to validate
        ({}, None, None, False),
        ({}, True, True, False),
        # Validation supported by backend, default behavior -> validate
        ({"validation": True}, None, None, True),
        # (Validation supported by backend) no explicit validation enabled: follow auto_validate setting
        ({"validation": True}, True, None, True),
        ({"validation": True}, False, None, False),
        # (Validation supported by backend) follow explicit `validate` toggle regardless of auto_validate
        ({"validation": True}, False, True, True),
        ({"validation": True}, True, False, False),
    ]

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_cube_download_validation(self, dummy_backend, connection, validate, validation_expected, caplog, tmp_path):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        cube = connection.load_collection("S2")

        output = tmp_path / "result.tiff"
        cube.download(outputfile=output, **dict_no_none(validate=validate))
        assert output.read_bytes() == b'{"what?": "Result data"}'
        assert dummy_backend.get_sync_pg() == self._PG_S2_SAVE

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_S2_SAVE]
            assert caplog.messages == ["Preflight process graph validation raised: [NoAdd] Don't add numbers"]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []

    @pytest.mark.parametrize("api_capabilities", [{"validation": True}])
    def test_cube_download_validation_broken(self, dummy_backend, connection, requests_mock, caplog, tmp_path):
        """Test resilience against broken validation response."""
        requests_mock.post(
            connection.build_url("/validation"), status_code=500, json={"code": "Internal", "message": "nope!"}
        )

        cube = connection.load_collection("S2")

        output = tmp_path / "result.tiff"
        cube.download(outputfile=output, validate=True)
        assert output.read_bytes() == b'{"what?": "Result data"}'
        assert dummy_backend.get_sync_pg() == self._PG_S2_SAVE

        assert caplog.messages == ["Preflight process graph validation failed: [500] Internal: nope!"]

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_cube_execute_validation(self, dummy_backend, connection, validate, validation_expected, caplog):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        cube = connection.load_collection("S2")

        res = cube.execute(**dict_no_none(validate=validate))
        assert res == {"what?": "Result data"}
        assert dummy_backend.get_sync_pg() == self._PG_S2

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_S2]
            assert caplog.messages == ["Preflight process graph validation raised: [NoAdd] Don't add numbers"]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_cube_create_job_validation(
        self, dummy_backend, connection: Connection, validate, validation_expected, caplog
    ):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        cube = connection.load_collection("S2")
        job = cube.create_job(**dict_no_none(validate=validate))
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self._PG_S2_SAVE

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_S2_SAVE]
            assert caplog.messages == ["Preflight process graph validation raised: [NoAdd] Don't add numbers"]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []

    @pytest.mark.parametrize("api_capabilities", [{"validation": True}])
    def test_cube_create_job_validation_broken(self, dummy_backend, connection, requests_mock, caplog, tmp_path):
        """Test resilience against broken validation response."""
        requests_mock.post(
            connection.build_url("/validation"), status_code=500, json={"code": "Internal", "message": "nope!"}
        )

        cube = connection.load_collection("S2")
        job = cube.create_job(validate=True)
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self._PG_S2_SAVE

        assert caplog.messages == ["Preflight process graph validation failed: [500] Internal: nope!"]

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_cube_execute_batch_validation(self, dummy_backend, connection, validate, validation_expected, caplog):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        cube = connection.load_collection("S2")
        job = cube.execute_batch(**dict_no_none(validate=validate))
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self._PG_S2_SAVE

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_S2_SAVE]
            assert caplog.messages == ["Preflight process graph validation raised: [NoAdd] Don't add numbers"]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []


def test_execute_batch_with_title(s2cube, dummy_backend):
    """
    Support title/description in execute_batch
    https://github.com/Open-EO/openeo-python-client/issues/652
    """
    s2cube.execute_batch(title="S2 job", description="Lorem ipsum dolor S2 amet")
    assert dummy_backend.batch_jobs == {
        "job-000": {
            "job_id": "job-000",
            "pg": {
                "loadcollection1": {
                    "process_id": "load_collection",
                    "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
                },
                "saveresult1": {
                    "process_id": "save_result",
                    "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
                    "result": True,
                },
            },
            "status": "finished",
            "title": "S2 job",
            "description": "Lorem ipsum dolor S2 amet",
        }
    }


def test_cube_validate(con120, dummy_backend):
    dummy_backend.next_validation_errors = [{"code": "OddSupport", "message": "Odd values are not supported."}]
    cube = con120.load_collection("S2")
    result = cube.validate()

    assert dummy_backend.validation_requests == [
        {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
                "result": True,
            },
        }
    ]
    assert isinstance(result, ValidationResponse)
    assert result == [{"code": "OddSupport", "message": "Odd values are not supported."}]

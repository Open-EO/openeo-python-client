from typing import List, Union

import pytest

from openeo.extra.spectral_indices import (
    append_and_rescale_indices,
    append_index,
    append_indices,
    compute_and_rescale_indices,
    compute_index,
    compute_indices,
    list_indices,
    load_indices,
    load_constants,
)
from openeo.extra.spectral_indices.spectral_indices import _BandMapping, BandMappingException
from openeo.rest.datacube import DataCube


def _extract_process_nodes(cube: Union[dict, DataCube], process_id: str) -> List[dict]:
    """Extract process node(s) from a data cube or flat graph presentation by process_id"""
    if isinstance(cube, DataCube):
        cube = cube.flat_graph()
    return [d for d in cube.values() if d["process_id"] == process_id]


def test_load_indices():
    indices = load_indices()
    assert "NDVI" in indices
    assert indices["NDVI"]["formula"] == "(N - R)/(N + R)"
    assert indices["NDVI"]["application_domain"] == "vegetation"
    assert indices["NDVI"]["long_name"] == "Normalized Difference Vegetation Index"


def test_list_indices():
    indices = list_indices()
    assert "NDVI" in indices
    assert "NDWI" in indices
    assert "ANIR" in indices


def test_load_constants():
    constants = load_constants()
    assert constants["g"] == 2.5


class TestBandMapping:
    def test_get_platforms(self):
        band_mapping = _BandMapping()
        assert band_mapping.get_platforms() == {
            "landsat4",
            "landsat5",
            "landsat7",
            "landsat8",
            "landsat9",
            "modis",
            "planetscope",
            "sentinel1",
            "sentinel2",
            "sentinel2a",
            "sentinel2b",
            "wv2",
            "wv3",
        }

    def test_guess_platform(self):
        band_mapping = _BandMapping()
        assert band_mapping.guess_platform("SENTINEL2") == "sentinel2"
        assert band_mapping.guess_platform("SENTINEL2_L2A") == "sentinel2"
        assert band_mapping.guess_platform("SENTINEL1_GRD") == "sentinel1"
        assert band_mapping.guess_platform("Landsat4") == "landsat4"
        assert band_mapping.guess_platform("LANDSAT8-9_L2") == "landsat8"
        assert band_mapping.guess_platform("boa_landsat_8") == "landsat8"

    @pytest.mark.parametrize("platform", ["Landsat4", "Landsat5", "Landsat7"])
    def test_variable_to_band_map_landsat457(self, platform):
        band_mapping = _BandMapping()
        assert band_mapping.variable_to_band_name_map(platform) == {
            "B": "B1",
            "G": "B2",
            "N": "B4",
            "R": "B3",
            "S1": "B5",
            "S2": "B7",
            "T": "B6",
        }

    def test_variable_to_band_map_landsat8(self):
        band_mapping = _BandMapping()
        assert band_mapping.variable_to_band_name_map("LANDSAT8") == {
            "A": "B1",
            "B": "B2",
            "G": "B3",
            "N": "B5",
            "R": "B4",
            "S1": "B6",
            "S2": "B7",
            "T1": "B10",
            "T2": "B11",
        }

    def test_variable_to_band_map_modis(self):
        band_mapping = _BandMapping()
        assert band_mapping.variable_to_band_name_map("MODIS") == {
            "B": "B3",
            "G": "B4",
            "G1": "B11",
            "N": "B2",
            "R": "B1",
            "S1": "B6",
            "S2": "B7",
        }

    def test_variable_to_band_map_sentinel2(self):
        band_mapping = _BandMapping()
        assert band_mapping.variable_to_band_name_map("Sentinel2") == {
            "A": "B1",
            "B": "B2",
            "G": "B3",
            "N": "B8",
            "N2": "B8A",
            "R": "B4",
            "RE1": "B5",
            "RE2": "B6",
            "RE3": "B7",
            "S1": "B11",
            "S2": "B12",
            "WV": "B9",
        }

    def test_variable_to_band_map_sentinel1(self):
        band_mapping = _BandMapping()
        assert band_mapping.variable_to_band_name_map("Sentinel1") == {
            "HH": "HH",
            "HV": "HV",
            "VH": "VH",
            "VV": "VV",
        }

    def test_actual_band_name_to_variable_map_sentinel2(self):
        band_mapping = _BandMapping()
        assert band_mapping.actual_band_name_to_variable_map(
            platform="sentinel2", band_names=["B02", "B03", "B04", "B08"]
        ) == {"B02": "B", "B03": "G", "B04": "R", "B08": "N"}

    def test_actual_band_name_to_variable_map_landsat8(self):
        band_mapping = _BandMapping()
        assert band_mapping.actual_band_name_to_variable_map(
            platform="LANDSAT8", band_names=["B2", "B3", "B4", "B5"]
        ) == {"B2": "B", "B3": "G", "B4": "R", "B5": "N"}


@pytest.mark.parametrize("collection_id", ["SENTINEL2", "FOO_SENTINEL2_L2A"])
def test_compute_and_rescale_indices(con, collection_id):
    cube = con.load_collection(collection_id)

    index_dict = {
        "collection": {"input_range": [0, 8000], "output_range": [0, 250]},
        "indices": {
            "NDVI": {"input_range": [-1, 1], "output_range": [0, 250]},
            "NDMI": {"input_range": [-1, 1], "output_range": [0, 250]},
            "NDRE1": {"input_range": [-1, 1], "output_range": [0, 250]},
        },
    }
    indices = compute_and_rescale_indices(cube, index_dict)
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 7},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "arrayelement3": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 10},
        },
        "arrayelement4": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 4},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "subtract2": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "subtract3": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add2": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "add3": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "divide2": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract2"}, "y": {"from_node": "add2"}},
        },
        "divide3": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract3"}, "y": {"from_node": "add3"}},
        },
        "linearscalerange1": {
            "process_id": "linear_scale_range",
            "arguments": {
                "inputMax": 1,
                "inputMin": -1,
                "outputMax": 250,
                "outputMin": 0,
                "x": {"from_node": "divide1"},
            },
        },
        "linearscalerange2": {
            "process_id": "linear_scale_range",
            "arguments": {
                "inputMax": 1,
                "inputMin": -1,
                "outputMax": 250,
                "outputMin": 0,
                "x": {"from_node": "divide2"},
            },
        },
        "linearscalerange3": {
            "process_id": "linear_scale_range",
            "arguments": {
                "inputMax": 1,
                "inputMin": -1,
                "outputMax": 250,
                "outputMin": 0,
                "x": {"from_node": "divide3"},
            },
        },
        "arraycreate1": {
            "process_id": "array_create",
            "arguments": {
                "data": [
                    {"from_node": "linearscalerange1"},
                    {"from_node": "linearscalerange2"},
                    {"from_node": "linearscalerange3"},
                ]
            },
            "result": True,
        },
    }


@pytest.mark.parametrize("collection_id", ["SENTINEL2", "FOO_SENTINEL2_L2A"])
def test_append_and_rescale_indices(con, collection_id):
    cube = con.load_collection(collection_id)

    index_dict = {
        "collection": {"input_range": [0, 8000], "output_range": [0, 250]},
        "indices": {
            "NDVI": {"input_range": [-1, 1], "output_range": [0, 250]},
            "NDMI": {"input_range": [-1, 1], "output_range": [0, 250]},
            "NDRE1": {"input_range": [-1, 1], "output_range": [0, 250]},
        },
    }
    indices = append_and_rescale_indices(cube, index_dict)
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 7},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "arrayelement3": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 10},
        },
        "arrayelement4": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 4},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "subtract2": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "subtract3": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add2": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "add3": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "divide2": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract2"}, "y": {"from_node": "add2"}},
        },
        "divide3": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract3"}, "y": {"from_node": "add3"}},
        },
        "linearscalerange1": {
            "process_id": "linear_scale_range",
            "arguments": {
                "inputMin": 0,
                "inputMax": 8000,
                "outputMin": 0,
                "outputMax": 250,
                "x": {"from_parameter": "data"},
            },
        },
        "linearscalerange2": {
            "process_id": "linear_scale_range",
            "arguments": {
                "inputMax": 1,
                "inputMin": -1,
                "outputMax": 250,
                "outputMin": 0,
                "x": {"from_node": "divide1"},
            },
        },
        "linearscalerange3": {
            "process_id": "linear_scale_range",
            "arguments": {
                "inputMax": 1,
                "inputMin": -1,
                "outputMax": 250,
                "outputMin": 0,
                "x": {"from_node": "divide2"},
            },
        },
        "linearscalerange4": {
            "process_id": "linear_scale_range",
            "arguments": {
                "inputMax": 1,
                "inputMin": -1,
                "outputMax": 250,
                "outputMin": 0,
                "x": {"from_node": "divide3"},
            },
        },
        "arraymodify1": {
            "process_id": "array_modify",
            "arguments": {
                "data": {"from_node": "linearscalerange1"},
                "index": 12,
                "values": [
                    {"from_node": "linearscalerange2"},
                    {"from_node": "linearscalerange3"},
                    {"from_node": "linearscalerange4"},
                ],
            },
            "result": True,
        },
    }


@pytest.mark.parametrize("collection_id", ["SENTINEL2", "FOO_SENTINEL2_L2A"])
def test_compute_indices(con, collection_id):
    cube = con.load_collection(collection_id)

    index_list = ["NDVI", "NDMI", "NDRE1"]
    indices = compute_indices(cube, index_list)
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 7},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "arrayelement3": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 10},
        },
        "arrayelement4": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 4},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "subtract2": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "subtract3": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add2": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "add3": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "divide2": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract2"}, "y": {"from_node": "add2"}},
        },
        "divide3": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract3"}, "y": {"from_node": "add3"}},
        },
        "arraycreate1": {
            "process_id": "array_create",
            "arguments": {"data": [{"from_node": "divide1"}, {"from_node": "divide2"}, {"from_node": "divide3"}]},
            "result": True,
        },
    }


@pytest.mark.parametrize("collection_id", ["SENTINEL2", "FOO_SENTINEL2_L2A"])
def test_append_indices(con, collection_id):
    cube = con.load_collection(collection_id)

    index_list = ["NDVI", "NDMI", "NDRE1"]
    indices = append_indices(cube, index_list)
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 7},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "arrayelement3": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 10},
        },
        "arrayelement4": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 4},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "subtract2": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "subtract3": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add2": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement3"}},
        },
        "add3": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement4"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "divide2": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract2"}, "y": {"from_node": "add2"}},
        },
        "divide3": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract3"}, "y": {"from_node": "add3"}},
        },
        "arraymodify1": {
            "process_id": "array_modify",
            "arguments": {
                "data": {"from_parameter": "data"},
                "index": 12,
                "values": [{"from_node": "divide1"}, {"from_node": "divide2"}, {"from_node": "divide3"}],
            },
            "result": True,
        },
    }


@pytest.mark.parametrize(["collection_id", "nir_index"], [("SENTINEL2", 7), ("FOO_SENTINEL2_L2A", 7), ("LANDSAT8", 4)])
def test_compute_ndvi(con, collection_id, nir_index):
    cube = con.load_collection(collection_id)
    indices = compute_index(cube, index="NDVI")
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": nir_index},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "arraycreate1": {
            "process_id": "array_create",
            "arguments": {"data": [{"from_node": "divide1"}]},
            "result": True,
        },
    }


@pytest.mark.parametrize(
    ["collection_id", "platform", "nir_index"],
    [
        ("NELITENS2", "Sentinel2", 7),
        ("NELITENS2", "SENTINEL2", 7),
        ("SANDLAT8", "landsat8", 4),
    ],
)
def test_compute_ndvi_explicit_platform(con, collection_id, platform, nir_index):
    cube = con.load_collection(collection_id)
    with pytest.raises(BandMappingException, match=f"Unable to guess satellite platform from id '{collection_id}'"):
        _ = compute_index(cube, index="NDVI")

    indices = compute_index(cube, index="NDVI", platform=platform)
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": nir_index},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "arraycreate1": {
            "process_id": "array_create",
            "arguments": {"data": [{"from_node": "divide1"}]},
            "result": True,
        },
    }


@pytest.mark.parametrize(
    ["collection_id", "band_to_var", "nir_index"],
    [
        ("ZENDIMEL2", {"Z2-B04": "R", "Z2-B08": "N"}, 7),
        ("ZENDIMEL2", {"Z2-B02": "B", "Z2-B03": "G", "Z2-B04": "R", "Z2-B08": "N"}, 7),
        ("ZANDLAD8", {"Z8-B4": "R", "Z8-B5": "N"}, 4),
    ],
)
def test_compute_ndvi_explicit_band_to_var(con, collection_id, band_to_var, nir_index):
    cube = con.load_collection(collection_id)
    with pytest.raises(BandMappingException, match=f"Unable to guess satellite platform from id '{collection_id}'"):
        _ = compute_index(cube, index="NDVI")

    indices = compute_index(cube, index="NDVI", band_to_var=band_to_var)
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": nir_index},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "arraycreate1": {
            "process_id": "array_create",
            "arguments": {"data": [{"from_node": "divide1"}]},
            "result": True,
        },
    }


@pytest.mark.parametrize("collection_id", ["SENTINEL2", "FOO_SENTINEL2_L2A"])
def test_compute_evi(con, collection_id):
    cube = con.load_collection(collection_id)
    indices = compute_index(cube, index="EVI")
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        # band at 7: NIR
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 7},
        },
        # band at 3: RED
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        # band at 1: BLUE
        "arrayelement3": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 1},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "multiply2"}},
        },
        "add2": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "subtract2"}, "y": 1.0},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "multiply1"}, "y": {"from_node": "add2"}},
        },
        "multiply1": {
            "process_id": "multiply",
            "arguments": {"x": 2.5, "y": {"from_node": "subtract1"}},
        },
        "multiply2": {
            "process_id": "multiply",
            "arguments": {"x": 6.0, "y": {"from_node": "arrayelement2"}},
        },
        "multiply3": {
            "process_id": "multiply",
            "arguments": {"x": 7.5, "y": {"from_node": "arrayelement3"}},
        },
        "subtract2": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "add1"}, "y": {"from_node": "multiply3"}},
        },
        "arraycreate1": {
            "process_id": "array_create",
            "arguments": {"data": [{"from_node": "divide1"}]},
            "result": True,
        },
    }


@pytest.mark.parametrize("collection_id", ["SENTINEL2", "FOO_SENTINEL2_L2A"])
def test_append_ndvi(con, collection_id):
    cube = con.load_collection(collection_id)
    indices = append_index(cube, index="NDVI")
    (apply_dim,) = _extract_process_nodes(indices, "apply_dimension")
    assert apply_dim["arguments"]["process"]["process_graph"] == {
        "arrayelement1": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 7},
        },
        "arrayelement2": {
            "process_id": "array_element",
            "arguments": {"data": {"from_parameter": "data"}, "index": 3},
        },
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": {"from_node": "add1"}},
        },
        "arraymodify1": {
            "process_id": "array_modify",
            "arguments": {
                "data": {"from_parameter": "data"},
                "index": 12,
                "values": [
                    {"from_node": "divide1"},
                ],
            },
            "result": True,
        },
    }

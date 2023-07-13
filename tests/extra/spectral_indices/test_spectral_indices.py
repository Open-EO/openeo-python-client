from typing import List, Union

from openeo.extra.spectral_indices import (
    append_and_rescale_indices,
    compute_and_rescale_indices,
    compute_indices,
    append_indices,
    compute_index,
    append_index,
    list_indices,
    load_indices,
)
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


def test_compute_and_rescale_indices(con):
    cube = con.load_collection("Sentinel2")

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


def test_append_and_rescale_indices(con):
    cube = con.load_collection("Sentinel2")

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


def test_compute_indices(con):
    cube = con.load_collection("Sentinel2")

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


def test_append_indices(con):
    cube = con.load_collection("Sentinel2")

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


def test_compute_index(con):
    cube = con.load_collection("Sentinel2")

    index = "NDVI"
    indices = compute_index(cube, index)
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
        "arraycreate1": {
            "process_id": "array_create",
            "arguments": {
                "data": [
                    {"from_node": "divide1"},
                ]
            },
            "result": True,
        },
    }


def test_append_index(con):
    cube = con.load_collection("Sentinel2")

    index = "NDVI"
    indices = append_index(cube, index)
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

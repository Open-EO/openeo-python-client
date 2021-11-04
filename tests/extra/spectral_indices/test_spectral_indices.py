from typing import List, Union

from openeo.extra.spectral_indices.spectral_indices import compute_indices
from openeo.rest.datacube import DataCube


def _extract_process_nodes(cube: Union[dict, DataCube], process_id: str) -> List[dict]:
    """Extract process node(s) from a data cube or flat graph presentation by process_id"""
    if isinstance(cube, DataCube):
        cube = cube.flat_graph()
    return [d for d in cube.values() if d["process_id"] == process_id]


def test_simple_ndvi(con):
    cube = con.load_collection("Sentinel2")
    indices = compute_indices(cube, ["NDVI"])
    apply_dim, = _extract_process_nodes(indices, "apply_dimension")
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
            "arguments": {"data": {"from_parameter": "data"}, "index": 12, "values": [{"from_node": "divide1"}]},
            "result": True,
        },
    }

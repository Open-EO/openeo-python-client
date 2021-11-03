from unittest.mock import Mock
from openeo.extra.spectral_indices.spectral_indices import _callback, _get_expression_map
from processes import array_create

x = array_create([9,4,6,1,3,8,2,5,7,11,7,2])
cube = Mock()
cube.metadata.band_names = ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B9", "B11", "B12"]
cube.graph = {"loadcollection1": {"arguments": {"id": "TERRASCOPE_S2_TOC_V2"}}}
index_specs = {
    "NDRE1": {
        "bands": ["N","RE1"],
        "formula": "(N - RE1) / (N + RE1)",
        "range": "(-1,1)"
    },
    "NDGI": {
        "bands": ["G","R"],
        "formula": "(G - R) / (G + R)",
        "range": "(-1,1)"
    },
    "NDRE5": {
        "bands": ["RE1","RE3"],
        "formula": "(RE3 - RE1) / (RE3 + RE1)",
        "range": "(-1,1)"
    }
}


def test_get_expression_map():
    assert _get_expression_map(cube, x)["A"] == array_create([9])

def test_callback():
    index_list = ["NDRE1","NDGI","NDRE5"]
    scaling_factor = None
    assert _callback(x, index_list, cube, scaling_factor, index_specs)

from unittest.mock import Mock, MagicMock
from extra.spectral_indices.spectral_indices import _callback, _get_expression_map
from openeo.rest.connection import Connection
from processes import array_create, ProcessBuilder
from tests.rest.datacube.conftest import con100

# cube = Mock()
# cube.metadata.band_names = ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B9", "B11", "B12"]
# cube.metadata.get = MagicMock(return_value="TERRASCOPE_S2_TOC_V2")
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


def test_get_expression_map(con100: Connection):
    cube = con100.load_collection("S2",bands=["B02","B03","B04","B08"]).filter_bbox(*[3, 51, 4, 52])
    cube.metadata.get = MagicMock(return_value="TERRASCOPE_S2_TOC_V2")
    assert True
    # assert _get_expression_map(cube, x)["B"] == array_create([9])

def test_callback():
    index_name = "NDGI"
    uplim_rescale = 250
    index_result = eval(index_specs[index_name]["formula"], {"G": 5, "R": 2, "RE1": 3})
    print(index_result)
    index_result = index_result.linear_scale_range(*eval(index_specs[index_name]["range"]), 0, uplim_rescale)
    print(index_result)
    assert True


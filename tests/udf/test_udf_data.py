import numpy
import xarray

from openeo.udf.structured_data import StructuredData
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube


def test_structured_data_list():
    sd1 = StructuredData([1, 2, 3, 5, 8])
    sd2 = StructuredData({"a": [3, 5], "b": "red"})
    udf_data = UdfData(structured_data_list=[sd1, sd2])

    assert udf_data.to_dict() == {
        "datacubes": [],
        "structured_data_list": [
            {"data": [1, 2, 3, 5, 8], "description": "list", "type": "list"},
            {"data": {"a": [3, 5], "b": "red"}, "description": "dict", "type": "dict"}
        ],
        "proj": None,
        "user_context": {}
    }


def test_datacube_list():
    xa = xarray.DataArray(numpy.zeros((2, 3)), coords={"x": [1, 2], "y": [3, 4, 5]}, dims=("x", "y"), name="testdata")
    cube = XarrayDataCube(xa)
    udf_data = UdfData(datacube_list=[cube], user_context={"kernel": 3})
    assert udf_data.to_dict() == {
        "datacubes": [
            {
                "id": "testdata",
                "data": [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0]],
                "dimensions": [
                    {"name": "x", "coordinates": [1, 2]},
                    {"name": "y", "coordinates": [3, 4, 5]}
                ],
            }],
        "structured_data_list": [],
        "proj": None,
        "user_context": {"kernel": 3}
    }


def test_udf_data_from_dict_empty():
    udf_data = UdfData.from_dict({})
    assert udf_data.to_dict() == {
        'datacubes': [],
        'structured_data_list': [],
        'proj': None, 'user_context': {},
    }


def test_udf_data_from_dict_structured_data():
    udf_data = UdfData.from_dict({"structured_data_list": [{"data": [1, 2, 3]}]})
    assert udf_data.to_dict() == {
        'datacubes': [],
        'structured_data_list': [{"data": [1, 2, 3], "type": "list", "description": "list"}],
        'proj': None, 'user_context': {},
    }


def test_udf_data_from_dict_datacube():
    udf_data = UdfData.from_dict({
        "datacubes": [
            {"data": [1, 2, 3], "dimensions": [{"name": "x"}]}
        ]
    })
    assert udf_data.to_dict() == {
        'datacubes': [{"data": [1, 2, 3], "dimensions": [{"name": "x"}]}],
        'structured_data_list': [],
        'proj': None, 'user_context': {},
    }

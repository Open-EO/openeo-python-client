import numpy
import xarray
from geopandas import GeoDataFrame
from shapely.geometry import Point

from openeo.udf import StructuredData, UdfData, XarrayDataCube, FeatureCollection


def test_structured_data_list():
    sd1 = StructuredData([1, 2, 3, 5, 8])
    sd2 = StructuredData({"a": [3, 5], "b": "red"})
    udf_data = UdfData(structured_data_list=[sd1, sd2])

    assert udf_data.to_dict() == {
        "datacubes": [],
        "feature_collection_list": [],
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
        "feature_collection_list": [],
        "structured_data_list": [],
        "proj": None,
        "user_context": {"kernel": 3}
    }


def test_feature_collection_list():
    data = GeoDataFrame({"a": [1, 4], "b": [2, 16]}, geometry=[Point(1, 2), Point(3, 5)])
    fc = FeatureCollection(id="test", data=data)
    udf_data = UdfData(feature_collection_list=[fc])
    assert udf_data.to_dict() == {
        'datacubes': [],
        'feature_collection_list': [{
            'data': {
                'type': 'FeatureCollection',
                'features': [
                    {'id': '0', 'type': 'Feature', 'geometry': {'coordinates': (1.0, 2.0), 'type': 'Point'},
                     'properties': {'a': 1, 'b': 2}, 'bbox': (1.0, 2.0, 1.0, 2.0), },
                    {'id': '1', 'type': 'Feature', 'geometry': {'coordinates': (3.0, 5.0), 'type': 'Point'},
                     'properties': {'a': 4, 'b': 16}, 'bbox': (3.0, 5.0, 3.0, 5.0), }
                ],
                'bbox': (1.0, 2.0, 3.0, 5.0),
            },
            'id': 'test'}
        ],
        'proj': None,
        'structured_data_list': [],
        'user_context': {}
    }


def test_udf_data_from_dict_empty():
    udf_data = UdfData.from_dict({})
    assert udf_data.to_dict() == {
        'datacubes': [],
        "feature_collection_list": [],
        'structured_data_list': [],
        'proj': None, 'user_context': {},
    }


def test_udf_data_from_dict_structured_data():
    udf_data = UdfData.from_dict({"structured_data_list": [{"data": [1, 2, 3]}]})
    assert udf_data.to_dict() == {
        'datacubes': [],
        "feature_collection_list": [],
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
        "feature_collection_list": [],
        'structured_data_list': [],
        'proj': None, 'user_context': {},
    }

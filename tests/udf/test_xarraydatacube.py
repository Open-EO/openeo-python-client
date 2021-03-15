import numpy
import xarray
import xarray.testing

from openeo.udf.xarraydatacube import XarrayDataCube


def test_xarraydatacube_to_dict_minimal():
    array = xarray.DataArray(numpy.zeros(shape=(2, 3)))
    xdc = XarrayDataCube(array=array)
    assert xdc.to_dict() == {
        "data": [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0]],
        "dimensions": [{"name": "dim_0"}, {"name": "dim_1"}],
    }


def test_xarraydatacube_from_dict_minimal():
    d = {"data": [[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]]}
    xdc = XarrayDataCube.from_dict(d)
    assert isinstance(xdc, XarrayDataCube)
    assert xdc.id is None
    xarray.testing.assert_equal(xdc.get_array(), xarray.DataArray([[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]]))
    assert xdc.to_dict() == {
        "data": [[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]],
        "dimensions": [{"name": "dim_0"}, {"name": "dim_1"}],
    }


def test_xarraydatacube_to_dict():
    array = xarray.DataArray(
        numpy.zeros(shape=(2, 3)), coords={'x': [1, 2], 'y': [1, 2, 3]}, dims=('x', 'y'),
        name="testdata",
        attrs={"description": "This is an xarray with two dimensions"},
    )
    xdc = XarrayDataCube(array=array)
    assert xdc.to_dict() == {
        "id": "testdata",
        "description": 'This is an xarray with two dimensions',
        "data": [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0]],
        "dimensions": [
            {'name': 'x', 'coordinates': [1, 2]},
            {'name': 'y', 'coordinates': [1, 2, 3]},
        ],
    }


def test_xarray_datacube_from_dict():
    d = {
        "id": "testdata",
        "description": 'This is an xarray with two dimensions',
        "data": [[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]],
        "dimensions": [
            {'name': 'x', 'coordinates': [1, 2]},
            {'name': 'y', 'coordinates': [1, 2, 3]},
        ],
    }
    xdc = XarrayDataCube.from_dict(d)
    assert isinstance(xdc, XarrayDataCube)
    assert xdc.to_dict() == d

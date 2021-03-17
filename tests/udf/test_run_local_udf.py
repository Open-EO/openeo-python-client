from pathlib import Path

import numpy
import xarray

from openeo.rest.conversions import datacube_to_file
from openeo.rest.datacube import DataCube
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube
from .. import as_path

UDF_CODE_PATH = Path(__file__).parent / "udf_code"


def _build_data() -> XarrayDataCube:
    t_coords = [numpy.datetime64('2020-08-01'), numpy.datetime64('2020-08-11'), numpy.datetime64('2020-08-21')]
    b_coords = ['bandzero', 'bandone']
    x_coords = [10., 11., 12., 13., 14.]
    y_coords = [20., 21., 22., 23., 24., 25.]

    t, b, x, y = numpy.ogrid[0:len(t_coords), 0:len(b_coords), 0:len(x_coords), 0:len(y_coords)]
    a = (1000 * t + 100 * b + 10 * x + y).astype(numpy.int32)

    return XarrayDataCube(xarray.DataArray(
        a,
        dims=['t', 'bands', 'x', 'y'],
        coords={"t": t_coords, "bands": b_coords, "x": x_coords, "y": y_coords}
    ))


def _get_udf_code(filename: str):
    with (UDF_CODE_PATH / filename).open("r") as f:
        return f.read()


def _ndvi(red, nir):
    return (nir - red) / (nir + red)


def test_run_local_udf_basic():
    xdc = _build_data()
    udf_code = _get_udf_code("ndvi01.py")
    res = DataCube.execute_local_udf(udf_code, xdc)

    assert isinstance(res, UdfData)
    result = res.get_datacube_list()[0].get_array()

    assert result.shape == (3, 1, 5, 6)
    expected = xarray.DataArray(
        [
            [_ndvi(0, 100), _ndvi(1, 101)],
            [_ndvi(10, 110), _ndvi(11, 111)]
        ],
        dims=["x", "y"],
        coords={"t": numpy.datetime64("2020-08-01"), "bands": "ndvi"}
    )
    xarray.testing.assert_equal(result[0, 0, 0:2, 0:2], expected)

    assert result[2, 0, 3, 4] == _ndvi(2034, 2134)


def test_run_local_udf_from_file_json(tmp_path):
    udf_code = _get_udf_code("ndvi01.py")
    xdc = _build_data()
    data_path = as_path(tmp_path / "data.json")
    datacube_to_file(xdc, data_path, fmt="json")

    res = DataCube.execute_local_udf(udf_code, data_path, fmt="json")

    assert isinstance(res, UdfData)
    result = res.get_datacube_list()[0].get_array()

    assert result.shape == (3, 1, 5, 6)
    expected = xarray.DataArray(
        [
            [_ndvi(0, 100), _ndvi(1, 101)],
            [_ndvi(10, 110), _ndvi(11, 111)]
        ],
        dims=["x", "y"],
        coords={"t": numpy.datetime64("2020-08-01"), "bands": "ndvi"}
    )
    xarray.testing.assert_equal(result[0, 0, 0:2, 0:2], expected)

    assert result[2, 0, 3, 4] == _ndvi(2034, 2134)


def test_run_local_udf_from_file_netcdf(tmp_path):
    udf_code = _get_udf_code("ndvi01.py")
    xdc = _build_data()
    data_path = as_path(tmp_path / "data.nc")
    datacube_to_file(xdc, data_path, fmt="netcdf")

    res = DataCube.execute_local_udf(udf_code, data_path, fmt="netcdf")

    assert isinstance(res, UdfData)
    result = res.get_datacube_list()[0].get_array()

    assert result.shape == (3, 1, 5, 6)
    expected = xarray.DataArray(
        [
            [_ndvi(0, 100), _ndvi(1, 101)],
            [_ndvi(10, 110), _ndvi(11, 111)]
        ],
        dims=["x", "y"],
        coords={"t": numpy.datetime64("2020-08-01"), "bands": "ndvi"}
    )
    xarray.testing.assert_equal(result[0, 0, 0:2, 0:2], expected)

    assert result[2, 0, 3, 4] == _ndvi(2034, 2134)

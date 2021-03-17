from pathlib import Path

import numpy
import pandas
import pytest
import xarray

from openeo.udf.run_code import run_udf_code, _get_annotation_str, _annotation_is_pandas_series, \
    _annotation_is_udf_datacube, \
    _annotation_is_udf_data
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube

UDF_CODE_PATH = Path(__file__).parent / "udf_code"


@pytest.mark.parametrize(["annotation", "expected"], [
    ("str", "str"),
    (pandas.Series, "pandas.core.series.Series"),
    (XarrayDataCube, "openeo.udf.xarraydatacube.XarrayDataCube"),
    (UdfData, "openeo.udf.udf_data.UdfData"),
    (str, "str"),
    (list, "list"),
])
def test_get_annotation_str(annotation, expected):
    assert _get_annotation_str(annotation) == expected


def test_annotation_is_pandas_series():
    assert _annotation_is_pandas_series(pandas.Series) is True
    assert _annotation_is_pandas_series("pandas.core.series.Series") is True


def test_annotation_is_udf_datacube():
    assert _annotation_is_udf_datacube(XarrayDataCube) is True
    assert _annotation_is_udf_datacube("openeo.udf.xarraydatacube.XarrayDataCube") is True
    assert _annotation_is_udf_datacube("openeo_udf.api.datacube.DataCube") is True


def test_annotation_is_udf_data():
    assert _annotation_is_udf_data(UdfData) is True
    assert _annotation_is_udf_data("openeo.udf.udf_data.UdfData") is True
    assert _annotation_is_udf_data("openeo_udf.api.udf_data.UdfData") is True


def _get_udf_code(filename: str):
    with (UDF_CODE_PATH / filename).open("r") as f:
        return f.read()


def _build_txy_data(
        ts: list, xs: list, ys: list, name: str,
        offset=0, t_factor=100, x_factor=10, y_factor=1
) -> XarrayDataCube:
    """Build `XarrayDataCube` with given t, x and y labels"""
    t, x, y = numpy.ogrid[0:len(ts), 0:len(xs), 0:len(ys)]
    a = offset + t_factor * t + x_factor * x + y_factor * y
    return XarrayDataCube(xarray.DataArray(
        data=a,
        coords={"t": ts, "x": xs, "y": ys},
        dims=['t', 'x', 'y'],
        name=name,
    ))


def test_run_udf_code_map_fabs():
    udf_code = _get_udf_code("map_fabs.py")
    xdc = _build_txy_data(ts=[2019, 2020, 2021], xs=[2, 3, 4], ys=[10, 20, 30], name="temp", x_factor=-10)
    udf_data = UdfData(datacube_list=[xdc])
    result = run_udf_code(code=udf_code, data=udf_data)

    assert list(xdc.array[0, :2, :].values.ravel()) == [0, 1, 2, -10, -9, -8]
    output, = result.get_datacube_list()
    assert output.id == "temp_fabs"
    assert output.array.name == "temp_fabs"
    assert output.array.shape == (3, 3, 3)
    assert list(output.array[0, :2, :].values.ravel()) == [0, 1, 2, 10, 9, 8]


def test_run_udf_code_reduce_time_mean():
    udf_code = _get_udf_code("reduce_time_mean.py")
    a = _build_txy_data(ts=[2018, 2019, 2020, 2021], xs=[2, 3], ys=[10, 20, 30], name="temp", offset=2)
    b = _build_txy_data(ts=[2018, 2019, 2020, 2021], xs=[2, 3], ys=[10, 20, 30], name="prec", offset=4)
    udf_data = UdfData(datacube_list=[a, b])
    result = run_udf_code(code=udf_code, data=udf_data)

    aa, bb = result.get_datacube_list()
    assert aa.id == "temp_mean"
    assert aa.array.name == "temp_mean"
    assert aa.array.shape == (2, 3)
    assert list(aa.array.values.ravel()) == [152.0, 153.0, 154.0, 162.0, 163.0, 164.0]

    assert bb.id == "prec_mean"
    assert bb.array.name == "prec_mean"
    assert bb.array.shape == (2, 3)
    assert list(bb.array.values.ravel()) == [154.0, 155.0, 156.0, 164.0, 165.0, 166.0]


def test_run_udf_code_reduce_time_min_median_max():
    udf_code = _get_udf_code("reduce_time_min_median_max.py")
    a = _build_txy_data(ts=[2018, 2019, 2020], xs=[2, 3], ys=[10, 20, 30], name="temp", offset=2)
    udf_data = UdfData(datacube_list=[a])
    result = run_udf_code(code=udf_code, data=udf_data)

    x, y, z = result.get_datacube_list()
    assert x.id == "temp_min"
    assert x.array.shape == (2, 3)
    assert list(x.array.values.ravel()) == [2, 3, 4, 12, 13, 14]

    assert y.id == "temp_median"
    assert y.array.shape == (2, 3)
    assert list(y.array.values.ravel()) == [102., 103., 104., 112., 113., 114.]

    assert z.id == "temp_max"
    assert z.array.shape == (2, 3)
    assert list(z.array.values.ravel()) == [202., 203., 204., 212., 213., 214.]


def test_run_udf_code_ndvi():
    udf_code = _get_udf_code("ndvi02.py")
    red = _build_txy_data(ts=[2018], xs=[0, 1], ys=[0, 1, 2], name="red", offset=2)
    nir = _build_txy_data(ts=[2018], xs=[0, 1], ys=[0, 1, 2], name="nir", offset=4)
    udf_data = UdfData(datacube_list=[red, nir])
    result = run_udf_code(code=udf_code, data=udf_data)

    print(nir.array)
    print(red.array)
    n, = result.get_datacube_list()
    assert n.id == "NDVI"
    assert n.array.shape == (1, 2, 3)

    def ndvi(red, nir):
        return (nir - red) / (nir + red)

    assert list(n.array.values.ravel()) == [
        ndvi(2, 4), ndvi(3, 5), ndvi(4, 6),
        ndvi(12, 14), ndvi(13, 15), ndvi(14, 16)
    ]


def test_run_udf_code_statistics():
    udf_code = _get_udf_code("statistics.py")
    xdc = _build_txy_data(ts=[2018, 2019], xs=[0, 1], ys=[0, 1, 2], name="temp", offset=2)
    udf_data = UdfData(datacube_list=[xdc])
    result = run_udf_code(code=udf_code, data=udf_data)

    structured, = result.structured_data_list
    assert structured.to_dict() == {
        "data": {"temp": {"min": 2, "mean": 58, "max": 114, "sum": 696}},
        "type": "dict",
        "description": "Statistical data sum, min, max and mean for each raster collection cube as dict"
    }

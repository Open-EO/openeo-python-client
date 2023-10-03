"""

General cube method tests against both
- 1.0.0-style DataCube

"""
import json
import pathlib
from datetime import date, datetime
from unittest import mock

import numpy as np
import pytest
import requests
import shapely
import shapely.geometry

from openeo.rest import BandMathException
from openeo.rest.connection import Connection
from openeo.rest.datacube import DataCube

from ... import load_json_resource
from .. import get_download_graph
from .conftest import API_URL


def test_apply_dimension_temporal_cumsum(s2cube, api_version):
    cumsum = s2cube.apply_dimension('cumsum', dimension="t")
    actual_graph = get_download_graph(cumsum)
    expected_graph = load_json_resource('data/{v}/apply_dimension_temporal_cumsum.json'.format(v=api_version))
    assert actual_graph == expected_graph


def test_apply_dimension_invalid_dimension_with_metadata(s2cube):
    with pytest.raises(ValueError, match="Invalid dimension"):
        s2cube.apply_dimension("cumsum", dimension="olapola")


def test_apply_dimension_invalid_dimension_no_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.apply_dimension("cumsum", dimension="olapola")
    assert get_download_graph(cube)["applydimension1"] == {
        "process_id": "apply_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "olapola",
            "process": {
                "process_graph": {
                    "cumsum1": {
                        "arguments": {"data": {"from_parameter": "data"}},
                        "process_id": "cumsum",
                        "result": True,
                    }
                }
            },
        },
    }


def test_min_time(s2cube, api_version):
    min_time = s2cube.min_time()
    actual_graph = get_download_graph(min_time)
    expected_graph = load_json_resource('data/{v}/min_time.json'.format(v=api_version))
    assert actual_graph == expected_graph


def _get_leaf_node(cube, force_flat=True) -> dict:
    """Get leaf node (node with result=True), supporting old and new style of graph building."""
    if isinstance(cube, DataCube):
        if force_flat:
            flat_graph = cube.flat_graph()
            node, = [n for n in flat_graph.values() if n.get("result")]
            return node
        else:
            return cube._pg.to_dict()
    else:
        raise ValueError(repr(cube))


def test_filter_temporal_basic_positional_args(s2cube):
    im = s2cube.filter_temporal("2016-01-01", "2016-03-10")
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_basic_start_end(s2cube):
    im = s2cube.filter_temporal(start_date="2016-01-01", end_date="2016-03-10")
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_basic_extent(s2cube):
    im = s2cube.filter_temporal(extent=("2016-01-01", "2016-03-10"))
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


@pytest.mark.parametrize("args,kwargs,extent", [
    ((), {}, [None, None]),
    (("2016-01-01",), {}, ["2016-01-01", None]),
    (("2016-01-01", "2016-03-10"), {}, ["2016-01-01", "2016-03-10"]),
    ((date(2016, 1, 1), date(2016, 3, 10)), {}, ["2016-01-01", "2016-03-10"]),
    ((datetime(2016, 1, 1, 12, 34), datetime(2016, 3, 10, 23, 45)), {},
     ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"]),
    ((), {"start_date": "2016-01-01", "end_date": "2016-03-10"}, ["2016-01-01", "2016-03-10"]),
    ((), {"start_date": "2016-01-01"}, ["2016-01-01", None]),
    ((), {"end_date": "2016-03-10"}, [None, "2016-03-10"]),
    ((), {"start_date": date(2016, 1, 1), "end_date": date(2016, 3, 10)}, ["2016-01-01", "2016-03-10"]),
    ((), {"start_date": datetime(2016, 1, 1, 12, 34), "end_date": datetime(2016, 3, 10, 23, 45)},
     ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"]),
    ((), {"extent": ("2016-01-01", "2016-03-10")}, ["2016-01-01", "2016-03-10"]),
    ((), {"extent": ("2016-01-01", None)}, ["2016-01-01", None]),
    ((), {"extent": (None, "2016-03-10")}, [None, "2016-03-10"]),
    ((), {"extent": (date(2016, 1, 1), date(2016, 3, 10))}, ["2016-01-01", "2016-03-10"]),
    ((), {"extent": (datetime(2016, 1, 1, 12, 34), datetime(2016, 3, 10, 23, 45))},
     ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"]),
])
def test_filter_temporal_generic(s2cube, args, kwargs, extent):
    im = s2cube.filter_temporal(*args, **kwargs)
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == extent


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        (["2016", None], ["2016-01-01", None]),
        (["2016-03", None], ["2016-03-01", None]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-03", "2016-04"], ["2016-03-01", "2016-04-01"]),
        (["2016-03", "2018-12"], ["2016-03-01", "2018-12-01"]),
        (["2016", "2016-01"], ["2016-01-01", "2016-01-01"]),
        (["2016", "2016-04"], ["2016-01-01", "2016-04-01"]),
        ([None, "2016-04"], [None, "2016-04-01"]),
    ],
)
def test_filter_temporal_extent_tuple_with_shorthand_dates(s2cube: DataCube, extent, expected):
    """Verify it supports shorthand dates that represent years or months."""
    im = s2cube.filter_temporal(extent=extent)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_temporal"
    assert graph["arguments"]["extent"] == expected


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        ("2016", ["2016-01-01", "2017-01-01"]),
        ("2016-01", ["2016-01-01", "2016-02-01"]),
        ("2016-04", ["2016-04-01", "2016-05-01"]),
        ("2016-12", ["2016-12-01", "2017-01-01"]),
        ("2016-04-11", ["2016-12-01", "2017-01-01"]),
    ],
)
def test_filter_temporal_extent_single_date_string(s2cube: DataCube, extent, expected):
    """Verify it supports single string extent."""
    im = s2cube.filter_temporal(extent=extent)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_temporal"
    assert graph["arguments"]["extent"] == expected


@pytest.mark.parametrize(
    "extent,expected",
    [
        (("2016-01-01", None), ["2016-01-01", None]),
        (("2016-01-01", "2016-03-10"), ["2016-01-01", "2016-03-10"]),
        ((None, "2016-03-10"), [None, "2016-03-10"]),
        (["2016-01-01", None], ["2016-01-01", None]),
    ],
)
def test_load_collection_filter_temporal(connection, api_version, extent, expected):
    cube: DataCube = connection.load_collection("S2", temporal_extent=extent)
    flat_graph = cube.flat_graph()
    assert flat_graph["loadcollection1"]["arguments"]["temporal_extent"] == expected


@pytest.mark.parametrize(
    "extent,expected",
    [
        # Test that the simplest/shortest syntax works: temporal_extent="2016"
        ("2016", ["2016-01-01", "2017-01-01"]),
        ("2016-02", ["2016-02-01", "2016-03-01"]),
        ("2016-02-03", ["2016-02-03", "2016-02-04"]),
        # Test date abbreviations using tuples for the extent
        (["2016", None], ["2016-01-01", None]),
        (["2016-02", None], ["2016-02-01", None]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-02", "2016-08"], ["2016-02-01", "2016-08-01"]),
        ([None, "2016"], [None, "2016-01-01"]),
        ([None, "2016-02"], [None, "2016-02-01"]),
    ],
)
def test_load_collection_temporal_extent_with_shorthand_date_strings(connection, api_version, extent, expected):
    """Verify it supports abbreviated date strings."""
    cube: DataCube = connection.load_collection("S2", temporal_extent=extent)
    flat_graph = cube.flat_graph()
    assert flat_graph["loadcollection1"]["arguments"]["temporal_extent"] == expected


def test_load_collection_bands_name(connection, api_version):
    im = connection.load_collection("S2", bands=["B08", "B04"])
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    assert im.flat_graph() == expected


def test_load_collection_bands_single_band(connection, api_version):
    im = connection.load_collection("S2", bands="B08")
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    expected["loadcollection1"]["arguments"]["bands"] = ["B08"]
    assert im.flat_graph() == expected


def test_load_collection_bands_common_name(connection, api_version):
    im = connection.load_collection("S2", bands=["nir", "red"])
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    expected["loadcollection1"]["arguments"]["bands"] = ["nir", "red"]
    assert im.flat_graph() == expected


def test_load_collection_bands_band_index(connection, api_version):
    im = connection.load_collection("S2", bands=[3, 2])
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    assert im.flat_graph() == expected


def test_load_collection_bands_and_band_math(connection, api_version):
    cube = connection.load_collection("S2", bands=["B03", "B04"])
    b4 = cube.band("B04")
    b3 = cube.band("B03")
    x = b4 - b3
    expected = load_json_resource('data/{v}/load_collection_bands_and_band_math.json'.format(v=api_version))
    assert x.flat_graph() == expected


def test_filter_bands_name(s2cube, api_version):
    im = s2cube.filter_bands(["B08", "B04"])
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08", "B04"]
    assert im.flat_graph() == expected


def test_filter_bands_single_band(s2cube, api_version):
    im = s2cube.filter_bands("B08")
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08"]
    assert im.flat_graph() == expected


def test_filter_bands_common_name(s2cube, api_version):
    im = s2cube.filter_bands(["nir", "red"])
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["nir", "red"]
    assert im.flat_graph() == expected


def test_filter_bands_index(s2cube, api_version):
    im = s2cube.filter_bands([3, 2])
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08", "B04"]
    assert im.flat_graph() == expected


def test_filter_bands_invalid_bands_with_metadata(s2cube):
    with pytest.raises(ValueError, match="Invalid band name/index 'apple'"):
        _ = s2cube.filter_bands(["apple", "banana"])


def test_filter_bands_invalid_bands_without_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.filter_bands(["apple", "banana"])
    assert get_download_graph(cube)["filterbands1"] == {
        "process_id": "filter_bands",
        "arguments": {"data": {"from_node": "loadcollection1"}, "bands": ["apple", "banana"]},
    }
    cube = cube.filter_bands(["banana"])
    assert get_download_graph(cube)["filterbands2"] == {
        "process_id": "filter_bands",
        "arguments": {"data": {"from_node": "filterbands1"}, "bands": ["banana"]},
    }


def test_filter_bbox_minimal(s2cube):
    im = s2cube.filter_bbox(west=3.0, east=3.1, north=51.1, south=51.0)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {"west": 3.0, "east": 3.1, "north": 51.1, "south": 51.0}


def test_filter_bbox_crs_4326(s2cube):
    im = s2cube.filter_bbox(west=3.0, east=3.1, north=51.1, south=51.0, crs=4326)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {"west": 3.0, "east": 3.1, "north": 51.1, "south": 51.0, "crs": 4326}


def test_filter_bbox_crs_32632(s2cube):
    im = s2cube.filter_bbox(
        west=652000, east=672000, north=5161000, south=5181000, crs=32632
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": 32632
    }


def test_filter_bbox_base_height(s2cube):
    im = s2cube.filter_bbox(
        west=652000, east=672000, north=5161000, south=5181000, crs=32632,
        base=100, height=200,
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": 32632,
        "base": 100, "height": 200,
    }


@pytest.mark.parametrize(["kwargs", "expected"], [
    ({}, {}),
    ({"crs": None}, {}),
    ({"crs": 4326}, {"crs": 4326}),
    ({"crs": 32632}, {"crs": 32632}),
    ({"base": None}, {}),
    ({"base": 123}, {"base": 123}),
    ({"height": None}, {}),
    ({"height": 456}, {"height": 456}),
    ({"base": None, "height": 456}, {"height": 456}),
    ({"base": 123, "height": 456}, {"base": 123, "height": 456}),
])
def test_filter_bbox_default_handling(s2cube, kwargs, expected):
    im = s2cube.filter_bbox(west=3, east=4, south=8, north=9, **kwargs)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == dict(west=3, east=4, south=8, north=9, **expected)


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        # test regular extents
        (("2016-01-01", None), ["2016-01-01", None]),
        (("2016-01-01", "2016-03-10"), ["2016-01-01", "2016-03-10"]),
        ((None, "2016-03-10"), [None, "2016-03-10"]),
        (["2016-01-01", None], ["2016-01-01", None]),
        # test the date abbreviations
        (["2016", None], ["2016-01-01", None]),
        (["2016-02", None], ["2016-02-01", None]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-02", "2016-02"], ["2016-02-01", "2016-02-01"]),
        (["2016", "2017"], ["2016-01-01", "2017-01-01"]),
        (["2016-02", "2016-08"], ["2016-02-01", "2016-08-01"]),
        ([None, "2016"], [None, "2016-01-01"]),
        ([None, "2016-02"], [None, "2016-02-01"]),
    ],
)
def test_filter_temporal_general(s2cube: DataCube, api_version, extent, expected):
    # First test it via positional args
    cube_pos_args: DataCube = s2cube.filter_temporal(extent[0], extent[1])
    flat_graph_pos_args = cube_pos_args.flat_graph()
    assert flat_graph_pos_args["filtertemporal1"]["arguments"]["extent"] == expected

    # Using start_date and end_date should give identical result
    cube_start_end: DataCube = s2cube.filter_temporal(start_date=extent[0], end_date=extent[1])
    flat_graph_start_end = cube_start_end.flat_graph()
    assert flat_graph_start_end["filtertemporal1"]["arguments"]["extent"] == expected

    # And using the extent parameter should also do exactly the same.
    cube_extent: DataCube = s2cube.filter_temporal(extent=extent)
    flat_graph_extent = cube_extent.flat_graph()
    assert flat_graph_extent["filtertemporal1"]["arguments"]["extent"] == expected


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        ("2016", ["2016-01-01", "2017-01-01"]),
        ("2016-02", ["2016-02-01", "2016-03-01"]),
        ("2016-12", ["2016-12-01", "2017-01-01"]),
        ("2016-03-04", ["2016-03-04", "2016-03-05"]),
    ],
)
def test_filter_temporal_extent_single_date_string(s2cube: DataCube, api_version, extent, expected):
    cube_extent: DataCube = s2cube.filter_temporal(extent=extent)
    flat_graph_extent = cube_extent.flat_graph()
    assert flat_graph_extent["filtertemporal1"]["arguments"]["extent"] == expected


def test_max_time(s2cube, api_version):
    im = s2cube.max_time()
    graph = _get_leaf_node(im, force_flat=True)
    assert graph["process_id"] == "reduce_dimension"
    assert graph["arguments"]["data"] == {"from_node": "loadcollection1"}
    assert graph["arguments"]["dimension"] == "t"
    callback = graph["arguments"]["reducer"]["process_graph"]["max1"]
    assert callback == {"arguments": {"data": {"from_parameter": "data"}}, "process_id": "max", "result": True}


def test_reduce_temporal_udf(s2cube, api_version):
    im = s2cube.reduce_temporal_udf("my custom code")
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "reduce_dimension"
    assert "data" in graph["arguments"]
    assert graph["arguments"]["dimension"] == "t"


def test_ndvi(s2cube, api_version):
    im = s2cube.ndvi()
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "ndvi"
    assert graph["arguments"] == {"data": {"from_node": "loadcollection1"}}


def test_mask_polygon(s2cube, api_version):
    polygon = shapely.geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
    expected_proces_id = "mask_polygon"
    im = s2cube.mask_polygon(mask=polygon)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == expected_proces_id
    assert graph["arguments"] == {
        "data": {'from_node': 'loadcollection1'},
        "mask": {
            'type': 'Polygon',
            'coordinates': (((0.0, 0.0), (1.9, 0.0), (1.9, 1.9), (0.0, 1.9), (0.0, 0.0)),),
        }
    }


def test_mask_raster(s2cube, connection, api_version):
    mask = connection.load_collection("MASK")
    im = s2cube.mask(mask=mask, replacement=102)
    graph = _get_leaf_node(im)
    assert graph == {
        "process_id": "mask",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "loadcollection2"},
            "replacement": 102,
        },
        "result": True,
    }


def test_apply_kernel(s2cube):
    kernel = [[0, 1, 0], [1, 1, 1], [0, 1, 0]]
    im = s2cube.apply_kernel(np.asarray(kernel), 3)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "apply_kernel"
    assert graph["arguments"] == {
        'data': {'from_node': 'loadcollection1'},
        'factor': 3,
        'border': 0,
        'replace_invalid': 0,
        'kernel': [[0, 1, 0], [1, 1, 1], [0, 1, 0]]
    }


def test_resample_spatial(s2cube):
    im = s2cube.resample_spatial(resolution=[2.0, 3.0], projection=4578)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "resample_spatial"
    assert "data" in graph["arguments"]
    assert graph["arguments"]["resolution"] == [2.0, 3.0]
    assert graph["arguments"]["projection"] == 4578


def test_merge(s2cube, api_version):
    merged = s2cube.ndvi().merge(s2cube)
    expected_graph = load_json_resource('data/{v}/merge_ndvi_self.json'.format(v=api_version))
    assert merged.flat_graph() == expected_graph


def test_apply_absolute_str(s2cube, api_version):
    result = s2cube.apply("absolute")
    expected_graph = load_json_resource('data/{v}/apply_absolute.json'.format(v=api_version))
    assert result.flat_graph() == expected_graph


def test_subtract_dates_ep3129(s2cube, api_version):
    """EP-3129: band math between cubes of different time stamps is not supported (yet?)"""
    bbox = {"west": 5.16, "south": 51.23, "east": 5.18, "north": 51.25}
    date1 = "2018-08-01"
    date2 = "2019-10-28"
    im1 = s2cube.filter_temporal(date1, date1).filter_bbox(**bbox).band('B04')
    im2 = s2cube.filter_temporal(date2, date2).filter_bbox(**bbox).band('B04')

    with pytest.raises(BandMathException, match="between bands of different"):
        im2.subtract(im1)


def test_tiled_viewing_service(s2cube, connection, requests_mock, api_version):
    expected_graph = load_json_resource('data/{v}/tiled_viewing_service.json'.format(v=api_version))

    def check_request(request):
        assert request.json() == expected_graph
        return True

    requests_mock.post(
        API_URL + "/services",
        status_code=201,
        text='',
        headers={'Location': API_URL + "/services/sf00", 'OpenEO-Identifier': 'sf00'},
        additional_matcher=check_request
    )

    res = s2cube.tiled_viewing_service(type="WMTS", title="S2 Foo", description="Nice!", custom_param=45)
    assert res.service_id == 'sf00'


def test_apply_dimension(connection, requests_mock):
    requests_mock.get(API_URL + "/collections/S22", json={
        "cube:dimensions": {
            "color": {"type": "bands", "values": ["cyan", "magenta", "yellow", "black"]},
            "alpha": {"type": "spatial"},
            "date": {"type": "temporal"}
        }
    })
    s22 = connection.load_collection("S22")

    for dim in ["color", "alpha", "date"]:
        cube = s22.apply_dimension(dimension=dim, code="subtract_mean")
        assert cube.flat_graph()["applydimension1"]["process_id"] == "apply_dimension"
        assert cube.flat_graph()["applydimension1"]["arguments"]["dimension"] == dim
    with pytest.raises(ValueError, match="Invalid dimension 'wut'"):
        s22.apply_dimension(dimension='wut', code="subtract_mean")


def test_download_path_str(connection, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + '/result', content=b"tiffdata")
    path = tmp_path / "tmp.tiff"
    connection.load_collection("S2").download(str(path), format="GTiff")
    assert path.read_bytes() == b"tiffdata"


def test_download_pathlib(connection, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + '/result', content=b"tiffdata")
    path = tmp_path / "tmp.tiff"
    connection.load_collection("S2").download(pathlib.Path(str(path)), format="GTIFF")
    assert path.read_bytes() == b"tiffdata"


@pytest.mark.parametrize(["filename", "expected_format"], [
    ("result.tiff", "GTiff"),
    ("result.tif", "GTiff"),
    ("result.gtiff", "GTiff"),
    ("result.geotiff", "GTiff"),
    ("result.nc", "netCDF"),
    ("result.netcdf", "netCDF"),
    ("result.csv", "CSV"),
])
@pytest.mark.parametrize("path_type", [str, pathlib.Path])
def test_download_format_guessing(
        connection, requests_mock, tmp_path, api_version, filename, path_type, expected_format
):
    requests_mock.get(API_URL + "/collections/S2", json={})

    def result_callback(request, context):
        post_data = request.json()
        pg = post_data["process"]["process_graph"]
        assert pg["saveresult1"]["arguments"]["format"] == expected_format
        return b"data"

    requests_mock.post(API_URL + '/result', content=result_callback)
    path = tmp_path / filename
    connection.load_collection("S2").download(path_type(path))
    assert path.read_bytes() == b"data"


@pytest.mark.parametrize(["format", "expected_format"], [
    ("GTiff", "GTiff"),
    ("netCDF", "netCDF"),
    (None, "GTiff"),
])
def test_download_bytes(connection, requests_mock, api_version, format, expected_format):
    requests_mock.get(API_URL + "/collections/S2", json={})

    def result_callback(request, context):
        post_data = request.json()
        pg = post_data["process"]["process_graph"]
        assert pg["saveresult1"]["arguments"]["format"] == expected_format
        return b"data"

    requests_mock.post(API_URL + '/result', content=result_callback)
    result = connection.load_collection("S2").download(format=format)
    assert result == b"data"


class TestExecuteBatch:
    @pytest.fixture
    def get_create_job_pg(self, connection):
        """Fixture to help intercepting the process graph that was passed to Connection.create_job"""
        with mock.patch.object(connection, "create_job") as create_job:

            def get() -> dict:
                assert create_job.call_count == 1
                return create_job.call_args[1]["process_graph"]

            yield get

    def test_create_job_defaults(self, s2cube, get_create_job_pg, recwarn, caplog):
        s2cube.create_job()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {},
            },
            "result": True,
        }
        assert recwarn.list == []
        assert caplog.records == []

    @pytest.mark.parametrize(
        ["out_format", "expected"],
        [("GTiff", "GTiff"), ("NetCDF", "NetCDF")],
    )
    def test_create_job_out_format(
        self, s2cube, get_create_job_pg, out_format, expected
    ):
        s2cube.create_job(out_format=out_format)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format", "expected"],
        [
            ("GTiff", "GTiff", "GTiff"),
            ("GTiff", None, "GTiff"),
            ("NetCDF", "NetCDF", "NetCDF"),
            ("NetCDF", None, "NetCDF"),
        ],
    )
    def test_create_job_existing_save_result(
        self,
        s2cube,
        get_create_job_pg,
        save_result_format,
        execute_format,
        expected,
    ):
        cube = s2cube.save_result(format=save_result_format)
        cube.create_job(out_format=execute_format)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format"],
        [("NetCDF", "GTiff"), ("GTiff", "NetCDF")],
    )
    def test_create_job_existing_save_result_incompatible(
        self, s2cube, save_result_format, execute_format
    ):
        cube = s2cube.save_result(format=save_result_format)
        with pytest.raises(ValueError):
            cube.create_job(out_format=execute_format)

    def test_execute_batch_defaults(self, s2cube, get_create_job_pg, recwarn, caplog):
        s2cube.execute_batch()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {},
            },
            "result": True,
        }
        assert recwarn.list == []
        assert caplog.records == []

    @pytest.mark.parametrize(
        ["out_format", "expected"],
        [("GTiff", "GTiff"), ("NetCDF", "NetCDF")],
    )
    def test_execute_batch_out_format(
        self, s2cube, get_create_job_pg, out_format, expected
    ):
        s2cube.execute_batch(out_format=out_format)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["output_file", "expected"],
        [("cube.tiff", "GTiff"), ("cube.nc", "netCDF")],
    )
    def test_execute_batch_out_format_from_output_file(
        self, s2cube, get_create_job_pg, output_file, expected
    ):
        s2cube.execute_batch(outputfile=output_file)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format", "expected"],
        [
            ("GTiff", "GTiff", "GTiff"),
            ("GTiff", None, "GTiff"),
            ("NetCDF", "NetCDF", "NetCDF"),
            ("NetCDF", None, "NetCDF"),
        ],
    )
    def test_execute_batch_existing_save_result(
        self,
        s2cube,
        get_create_job_pg,
        save_result_format,
        execute_format,
        expected,
    ):
        cube = s2cube.save_result(format=save_result_format)
        cube.execute_batch(out_format=execute_format)
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": expected,
                "options": {},
            },
            "result": True,
        }

    @pytest.mark.parametrize(
        ["save_result_format", "execute_format"],
        [("NetCDF", "GTiff"), ("GTiff", "NetCDF")],
    )
    def test_execute_batch_existing_save_result_incompatible(
        self, s2cube, save_result_format, execute_format
    ):
        cube = s2cube.save_result(format=save_result_format)
        with pytest.raises(ValueError):
            cube.execute_batch(out_format=execute_format)

    def test_save_result_format_options_vs_create_job(elf, s2cube, get_create_job_pg):
        """https://github.com/Open-EO/openeo-python-client/issues/433"""
        cube = s2cube.save_result(format="GTiff", options={"filename_prefix": "wwt-2023-02"})
        _ = cube.create_job()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {"filename_prefix": "wwt-2023-02"},
            },
            "result": True,
        }

    def test_save_result_format_options_vs_execute_batch(elf, s2cube, get_create_job_pg):
        """https://github.com/Open-EO/openeo-python-client/issues/433"""
        cube = s2cube.save_result(format="GTiff", options={"filename_prefix": "wwt-2023-02"})
        _ = cube.execute_batch()
        pg = get_create_job_pg()
        assert set(pg.keys()) == {"loadcollection1", "saveresult1"}
        assert pg["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {"filename_prefix": "wwt-2023-02"},
            },
            "result": True,
        }


class TestProcessGraphValidation:
    JOB_ID = "j-123"
    PROCESS_GRAPH_DICT = {"add1": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
    PROCESS_GRAPH_STRING = json.dumps(PROCESS_GRAPH_DICT)

    @pytest.fixture
    def cube_add(self, requests_mock, connection_with_pgvalidation_datacube: Connection) -> DataCube:
        requests_mock.post(API_URL + "/result", content=self._post_result_handler_json)
        return connection_with_pgvalidation_datacube.datacube_from_json(self.PROCESS_GRAPH_STRING)

    def _post_jobs_handler_json(self, response: requests.Request, context):
        context.headers["OpenEO-Identifier"] = self.JOB_ID
        return b""

    def _post_result_handler_json(self, response: requests.Request, context):
        pg = response.json()["process"]["process_graph"]
        assert pg == self.PROCESS_GRAPH_DICT
        return b'{"answer": 8}'

    @pytest.mark.parametrize("validate", [True, False])
    def test_create_job_with_pg_validation(
        self,
        requests_mock,
        connection_with_pgvalidation_datacube: Connection,
        validate,
    ):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        m = requests_mock.post(API_URL + "/validation", json={"errors": []})

        requests_mock.post(API_URL + "/jobs", status_code=201, content=self._post_jobs_handler_json)
        cube: DataCube = connection_with_pgvalidation_datacube.load_collection("S2")
        cube.create_job(validate=validate)

        # Validation should be called if and only if it was requested
        expected_call_count = 1 if validate else 0
        assert m.call_count == expected_call_count

    @pytest.mark.parametrize("validate", [True, False])
    def test_execute_with_pg_validation(
        self,
        requests_mock,
        cube_add: DataCube,
        validate,
    ):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        m = requests_mock.post(API_URL + "/validation", json={"errors": []})
        requests_mock.post(API_URL + "/jobs", status_code=201, content=self._post_jobs_handler_json)
        requests_mock.post(API_URL + "/result", content=self._post_result_handler_json)

        cube_add.execute(validate=validate)

        # Validation should be called if and only if it was requested
        expected_call_count = 1 if validate else 0
        assert m.call_count == expected_call_count

    @pytest.mark.parametrize("validate", [True, False])
    def test_execute_batch_with_pg_validation(
        self,
        requests_mock,
        cube_add: DataCube,
        validate,
    ):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        m = requests_mock.post(API_URL + "/validation", json={"errors": []})
        requests_mock.post(API_URL + "/jobs", status_code=201, content=self._post_jobs_handler_json)
        requests_mock.post(API_URL + f"/jobs/{self.JOB_ID}/results", status_code=202)
        job_metadata = {
            "id": self.JOB_ID,
            "title": f"Job {self.JOB_ID,}",
            "description": f"Job {self.JOB_ID,}",
            "process": self.PROCESS_GRAPH_DICT,
            "status": "finished",
            "created": "2017-01-01T09:32:12Z",
            "links": [],
        }
        requests_mock.get(API_URL + f"/jobs/{self.JOB_ID}", status_code=200, json=job_metadata)

        cube_add.execute_batch(validate=validate)

        # Validation should be called if and only if it was requested
        expected_call_count = 1 if validate else 0
        assert m.call_count == expected_call_count

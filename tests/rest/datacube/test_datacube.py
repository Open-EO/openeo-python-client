"""

General cube method tests against both
- 0.4.0-style ImageCollectionClient
- 1.0.0-style DataCube

"""

from datetime import date, datetime
import pathlib

import numpy as np
import pytest
import shapely
import shapely.geometry

from openeo.capabilities import ComparableVersion
from openeo.rest import BandMathException
from openeo.rest.datacube import DataCube
from openeo.rest.imagecollectionclient import ImageCollectionClient
from .conftest import API_URL
from .. import get_download_graph
from ..conftest import reset_graphbuilder
from ... import load_json_resource


def test_apply_dimension_temporal_cumsum(s2cube, api_version):
    cumsum = s2cube.apply_dimension('cumsum', dimension="t")
    actual_graph = get_download_graph(cumsum)
    expected_graph = load_json_resource('data/{v}/apply_dimension_temporal_cumsum.json'.format(v=api_version))
    assert actual_graph == expected_graph


def test_apply_dimension_invalid_dimension(s2cube):
    with pytest.raises(ValueError, match="Invalid dimension"):
        s2cube.apply_dimension('cumsum', dimension="olapola")


def test_min_time(s2cube, api_version):
    min_time = s2cube.min_time()
    actual_graph = get_download_graph(min_time)
    expected_graph = load_json_resource('data/{v}/min_time.json'.format(v=api_version))
    assert actual_graph == expected_graph


def _get_leaf_node(cube, force_flat=True) -> dict:
    """Get leaf node (node with result=True), supporting old and new style of graph building."""
    if isinstance(cube, ImageCollectionClient):
        return cube.graph[cube.node_id]
    elif isinstance(cube, DataCube):
        if force_flat:
            flat_graph = cube.flat_graph()
            node, = [n for n in flat_graph.values() if n.get("result")]
            return node
        else:
            return cube._pg.to_dict()
    else:
        raise ValueError(repr(cube))


def test_date_range_filter(s2cube):
    im = s2cube.date_range_filter("2016-01-01", "2016-03-10")
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_daterange(s2cube):
    im = s2cube.filter_daterange(extent=("2016-01-01", "2016-03-10"))
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal(s2cube):
    im = s2cube.filter_temporal("2016-01-01", "2016-03-10")
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_start_end(s2cube):
    im = s2cube.filter_temporal(start_date="2016-01-01", end_date="2016-03-10")
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_extent(s2cube):
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


def test_load_collection_bands_name(connection, api_version):
    im = connection.load_collection("S2", bands=["B08", "B04"])
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    assert im.graph == expected


def test_load_collection_bands_single_band(connection, api_version):
    im = connection.load_collection("S2", bands="B08")
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    expected["loadcollection1"]["arguments"]["bands"] = ["B08"]
    assert im.graph == expected


def test_load_collection_bands_common_name(connection, api_version):
    im = connection.load_collection("S2", bands=["nir", "red"])
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    if api_version < ComparableVersion("1.0.0"):
        expected["loadcollection1"]["arguments"]["bands"] = ["B08", "B04"]
    else:
        expected["loadcollection1"]["arguments"]["bands"] = ["nir", "red"]
    assert im.graph == expected


def test_load_collection_bands_band_index(connection, api_version):
    im = connection.load_collection("S2", bands=[3, 2])
    expected = load_json_resource('data/{v}/load_collection_bands.json'.format(v=api_version))
    assert im.graph == expected


def test_load_collection_bands_and_band_math(connection, api_version):
    cube = connection.load_collection("S2", bands=["B03", "B04"])
    b4 = cube.band("B04")
    b3 = cube.band("B03")
    x = b4 - b3
    expected = load_json_resource('data/{v}/load_collection_bands_and_band_math.json'.format(v=api_version))
    assert x.graph == expected


def test_filter_bands_name(s2cube, api_version):
    im = s2cube.filter_bands(["B08", "B04"])
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08", "B04"]
    assert im.graph == expected


def test_filter_bands_single_band(s2cube, api_version):
    im = s2cube.filter_bands("B08")
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08"]
    assert im.graph == expected


def test_filter_bands_common_name(s2cube, api_version):
    im = s2cube.filter_bands(["nir", "red"])
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    if api_version < ComparableVersion("1.0.0"):
        expected["filterbands1"]["arguments"]["bands"] = []
        expected["filterbands1"]["arguments"]["common_names"] = ["nir", "red"]
    else:
        expected["filterbands1"]["arguments"]["bands"] = ["nir", "red"]
    assert im.graph == expected


def test_filter_bands_index(s2cube, api_version):
    im = s2cube.filter_bands([3, 2])
    expected = load_json_resource('data/{v}/filter_bands.json'.format(v=api_version))
    expected["filterbands1"]["arguments"]["bands"] = ["B08", "B04"]
    assert im.graph == expected


def test_pipe(s2cube, api_version):
    def ndvi_percent(cube):
        return cube.ndvi().linear_scale_range(0, 1, 0, 100)

    im = s2cube.pipe(ndvi_percent)
    assert im.graph == load_json_resource('data/{v}/pipe.json'.format(v=api_version))


def test_pipe_with_args(s2cube):
    def ndvi_scaled(cube, in_max=2, out_max=3):
        return cube.ndvi().linear_scale_range(0, in_max, 0, out_max)

    reset_graphbuilder()
    im = s2cube.pipe(ndvi_scaled)
    assert im.graph["linearscalerange1"]["arguments"] == {
        'inputMax': 2, 'inputMin': 0, 'outputMax': 3, 'outputMin': 0, 'x': {'from_node': 'ndvi1'}
    }
    reset_graphbuilder()
    im = s2cube.pipe(ndvi_scaled, 4, 5)
    assert im.graph["linearscalerange1"]["arguments"] == {
        'inputMax': 4, 'inputMin': 0, 'outputMax': 5, 'outputMin': 0, 'x': {'from_node': 'ndvi1'}
    }
    reset_graphbuilder()
    im = s2cube.pipe(ndvi_scaled, out_max=7)
    assert im.graph["linearscalerange1"]["arguments"] == {
        'inputMax': 2, 'inputMin': 0, 'outputMax': 7, 'outputMin': 0, 'x': {'from_node': 'ndvi1'}
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


def test_bbox_filter_nsew(s2cube):
    # TODO: remove this test for deprecated `bbox_filter`
    im = s2cube.bbox_filter(
        west=652000, east=672000, north=5161000, south=5181000, crs=32632
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": 32632
    }


def test_bbox_filter_tblr(s2cube):
    # TODO: remove this test for deprecated `bbox_filter`
    im = s2cube.bbox_filter(
        left=652000, right=672000, top=5161000, bottom=5181000, srs=32632
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": 32632
    }


def test_bbox_filter_nsew_zero(s2cube):
    # TODO: remove this test for deprecated `bbox_filter`
    im = s2cube.bbox_filter(
        west=0, east=0, north=0, south=0, crs=32632
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 0, "east": 0, "north": 0, "south": 0, "crs": 32632
    }


def test_max_time(s2cube, api_version):
    im = s2cube.max_time()
    graph = _get_leaf_node(im, force_flat=True)
    assert graph["process_id"] == "reduce" if api_version == '0.4.0' else 'reduce_dimension'
    assert graph["arguments"]["data"] == {'from_node': 'loadcollection1'}
    assert graph["arguments"]["dimension"] == "t"
    if api_version == '0.4.0':
        callback = graph["arguments"]["reducer"]["callback"]["r1"]
        assert callback == {'arguments': {'data': {'from_argument': 'data'}}, 'process_id': 'max', 'result': True}
    else:
        callback = graph["arguments"]["reducer"]["process_graph"]["max1"]
        assert callback == {'arguments': {'data': {'from_parameter': 'data'}}, 'process_id': 'max', 'result': True}


def test_reduce_temporal_udf(s2cube, api_version):
    im = s2cube.reduce_temporal_udf("my custom code")
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "reduce" if api_version == '0.4.0' else 'reduce_dimension'
    assert "data" in graph["arguments"]
    assert graph["arguments"]["dimension"] == "t"


def test_ndvi(s2cube, api_version):
    im = s2cube.ndvi()
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "ndvi"
    if api_version == '0.4.0':
        assert graph["arguments"] == {'data': {'from_node': 'loadcollection1'}, 'name': 'ndvi'}
    else:
        assert graph["arguments"] == {'data': {'from_node': 'loadcollection1'}}


def test_mask_polygon(s2cube, api_version):
    polygon = shapely.geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
    if api_version < ComparableVersion("1.0.0"):
        expected_proces_id = "mask"
        im = s2cube.mask(polygon)
    else:
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
    if api_version == '0.4.0':
        im = s2cube.mask(rastermask=mask, replacement=102)
    else:
        im = s2cube.mask(mask=mask, replacement=102)
    graph = _get_leaf_node(im)
    assert graph == {
        "process_id": "mask",
        "arguments": {
            "data": {
                "from_node": "loadcollection1"
            },
            "mask": {
                "from_node": "loadcollection3" if api_version == '0.4.0' else "loadcollection2"
            },
            "replacement": 102
        },
        "result": False if api_version == '0.4.0' else True
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
    assert merged.graph == expected_graph


def test_apply_absolute_str(s2cube, api_version):
    result = s2cube.apply("absolute")
    expected_graph = load_json_resource('data/{v}/apply_absolute.json'.format(v=api_version))
    assert result.graph == expected_graph


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
        reset_graphbuilder()
        cube = s22.apply_dimension(dimension=dim, code="subtract_mean")
        assert cube.graph["applydimension1"]["process_id"] == "apply_dimension"
        assert cube.graph["applydimension1"]["arguments"]["dimension"] == dim
    with pytest.raises(ValueError, match="Invalid dimension 'wut'"):
        s22.apply_dimension(dimension='wut', code="subtract_mean")


def test_download_path_str(connection, requests_mock, tmp_path, api_version):
    requests_mock.get(API_URL + "/collections/S2", json={})

    def result_callback(request, context):
        post_data = request.json()
        pg = (post_data["process"] if api_version >= ComparableVersion("1.0.0") else post_data)["process_graph"]
        assert pg["saveresult1"]["arguments"]["format"] == "GTiff"
        return b"tiffdata"

    requests_mock.post(API_URL + '/result', content=result_callback)
    path = tmp_path / "tmp.tiff"
    connection.load_collection("S2").download(str(path), format="GTiff")
    assert path.read_bytes() == b"tiffdata"


def test_download_pathlib(connection, requests_mock, tmp_path, api_version):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + '/result', content=b"tiffdata")
    path = tmp_path / "tmp.tiff"
    connection.load_collection("S2").download(pathlib.Path(str(path)), format="GTIFF")
    assert path.read_bytes() == b"tiffdata"


def test_download_pathlib_no_format(connection, requests_mock, tmp_path, api_version):
    requests_mock.get(API_URL + "/collections/S2", json={})

    def result_callback(request, context):
        post_data = request.json()
        pg = (post_data["process"] if api_version >= ComparableVersion("1.0.0") else post_data)["process_graph"]
        assert pg["saveresult1"]["arguments"]["format"] == "GTiff"
        return b"tiffdata"

    requests_mock.post(API_URL + '/result', content=result_callback)
    path = tmp_path / "tmp.tiff"
    connection.load_collection("S2").download(pathlib.Path(str(path)))
    assert path.read_bytes() == b"tiffdata"


def test_download_bytes(connection, requests_mock):
    requests_mock.get(API_URL + "/collections/S2", json={})
    requests_mock.post(API_URL + '/result', content=b"tiffdata")
    result = connection.load_collection("S2").download(None, format="GTIFF")
    assert result == b"tiffdata"
    

def test_download_bytes_no_format(connection, requests_mock, api_version):
    requests_mock.get(API_URL + "/collections/S2", json={})

    def result_callback(request, context):
        post_data = request.json()
        pg = (post_data["process"] if api_version >= ComparableVersion("1.0.0") else post_data)["process_graph"]
        assert pg["saveresult1"]["arguments"]["format"] == "GTiff"
        return b"tiffdata"

    requests_mock.post(API_URL + '/result', content=result_callback)
    result = connection.load_collection("S2").download(None)
    assert result == b"tiffdata"

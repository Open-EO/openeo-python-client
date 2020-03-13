"""

General cube method tests against both
- 0.4.0-style ImageCollectionClient
- 1.0.0-style DataCube

"""

from datetime import date, datetime

import numpy as np
import pytest
import shapely
import shapely.geometry

from openeo.rest.datacube import DataCube
from openeo.rest.imagecollectionclient import ImageCollectionClient
from .. import get_download_graph
from ..conftest import reset_graphbuilder
from ... import load_json_resource


def test_apply_dimension_temporal_cumsum(s2cube, api_version):
    cumsum = s2cube.apply_dimension('cumsum')
    actual_graph = get_download_graph(cumsum)
    expected_graph = load_json_resource('data/{v}/apply_dimension_temporal_cumsum.json'.format(v=api_version))
    assert actual_graph == expected_graph


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
            flattened = cube.flatten()
            node, = [n for n in flattened.values() if n.get("result")]
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


def test_filter_bands(s2cube):
    im = s2cube.filter_bands(["red", "nir"])
    graph = _get_leaf_node(im)
    assert graph['process_id'] == 'filter_bands'
    assert graph['arguments']['bands'] == ["red", "nir"]


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


def test_filter_bbox(s2cube):
    im = s2cube.filter_bbox(
        west=652000, east=672000, north=5161000, south=5181000, crs="EPSG:32632"
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632"
    }


def test_filter_bbox_base_height(s2cube):
    im = s2cube.filter_bbox(
        west=652000, east=672000, north=5161000, south=5181000, crs="EPSG:32632",
        base=100, height=200,
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632",
        "base": 100, "height": 200,
    }


def test_bbox_filter_nsew(s2cube):
    im = s2cube.bbox_filter(
        west=652000, east=672000, north=5161000, south=5181000, crs="EPSG:32632"
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632"
    }


def test_bbox_filter_tblr(s2cube):
    im = s2cube.bbox_filter(
        left=652000, right=672000, top=5161000, bottom=5181000, srs="EPSG:32632"
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632"
    }


def test_bbox_filter_nsew_zero(s2cube):
    im = s2cube.bbox_filter(
        west=0, east=0, north=0, south=0, crs="EPSG:32632"
    )
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "filter_bbox"
    assert graph["arguments"]["extent"] == {
        "west": 0, "east": 0, "north": 0, "south": 0, "crs": "EPSG:32632"
    }


def test_max_time(s2cube, api_version):
    im = s2cube.max_time()
    graph = _get_leaf_node(im, force_flat=True)
    assert graph["process_id"] == "reduce" if api_version == '0.4.0' else 'reduce_dimension'
    assert graph["arguments"]["data"] == {'from_node': 'loadcollection1'}
    assert graph["arguments"]["dimension"] == "temporal"
    if api_version == '0.4.0':
        callback = graph["arguments"]["reducer"]["callback"]["r1"]
    else:
        callback = graph["arguments"]["reducer"]["process_graph"]["max1"]
    assert callback == {'arguments': {'data': {'from_argument': 'data'}}, 'process_id': 'max', 'result': True}


def test_reduce_time_udf(s2cube, api_version):
    im = s2cube.reduce_tiles_over_time("my custom code")
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "reduce" if api_version == '0.4.0' else 'reduce_dimension'
    assert "data" in graph["arguments"]


def test_ndvi(s2cube, api_version):
    im = s2cube.ndvi()
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "ndvi"
    if api_version == '0.4.0':
        assert graph["arguments"] == {'data': {'from_node': 'loadcollection1'}, 'name': 'ndvi'}
    else:
        assert graph["arguments"] == {'data': {'from_node': 'loadcollection1'}}


def test_mask(s2cube, api_version):
    polygon = shapely.geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
    if api_version == '0.4.0':
        im = s2cube.mask(polygon)
    else:
        im = s2cube.mask_polygon(polygon)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "mask"
    assert graph["arguments"] == {
        "data": {'from_node': 'loadcollection1'},
        "mask": {
            'coordinates': (((0.0, 0.0), (1.9, 0.0), (1.9, 1.9), (0.0, 1.9), (0.0, 0.0)),),
            'crs': {'properties': {'name': 'EPSG:4326'}, 'type': 'name'},
            'type': 'Polygon'
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


def test_stretch_colors(s2cube):
    im = s2cube.stretch_colors(-1, 1)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "stretch_colors"
    assert graph["arguments"] == {
        'data': {'from_node': 'loadcollection1'},
        'max': 1,
        'min': -1,
    }


def test_apply_kernel(s2cube):
    kernel = [[0, 1, 0], [1, 1, 1], [0, 1, 0]]
    im = s2cube.apply_kernel(np.asarray(kernel), 3)
    graph = _get_leaf_node(im)
    assert graph["process_id"] == "apply_kernel"
    assert graph["arguments"] == {
        'data': {'from_node': 'loadcollection1'},
        'factor': 3,
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


def test_apply_absolute(s2cube, api_version):
    result = s2cube.apply("absolute")
    expected_graph = load_json_resource('data/{v}/apply_absolute.json'.format(v=api_version))
    assert result.graph == expected_graph

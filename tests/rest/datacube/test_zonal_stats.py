import pytest
import shapely.geometry

import openeo.processes
from openeo.capabilities import ComparableVersion
from .. import get_execute_graph
from ... import load_json_resource


def test_polygon_timeseries_polygon(connection, api_version):
    polygon = shapely.geometry.shape(load_json_resource("data/polygon.json"))
    res = (
        connection
            .load_collection("S2")
            .filter_bbox(3, 6, 52, 50, "EPSG:4326")
            .polygonal_mean_timeseries(polygon)
    )
    assert get_execute_graph(res) == load_json_resource('data/%s/aggregate_zonal_polygon.json' % api_version)


@pytest.mark.parametrize("reducer", ["mean", openeo.processes.mean, lambda x: x.mean()])
def test_aggregate_spatial(connection, api_version, reducer):
    if api_version < ComparableVersion("1.0.0"):
        pytest.skip()
    polygon = shapely.geometry.shape(load_json_resource("data/polygon.json"))
    res = (
        connection.load_collection("S2")
            .filter_bbox(3, 6, 52, 50, "EPSG:4326")
            .aggregate_spatial(geometries=polygon, reducer=reducer)
    )
    assert get_execute_graph(res) == load_json_resource('data/%s/aggregate_zonal_polygon.json' % api_version)


def test_polygon_timeseries_path(connection, api_version):
    res = (
        connection.load_collection('S2')
            .bbox_filter(west=3, east=6, north=52, south=50, crs='EPSG:4326')
            .polygonal_mean_timeseries(polygon="/some/path/to/GeometryCollection.geojson")
    )
    assert get_execute_graph(res) == load_json_resource('data/%s/aggregate_zonal_path.json' % api_version)


@pytest.mark.parametrize("reducer", ["mean", openeo.processes.mean, lambda x: x.mean()])
def test_aggregate_spatial_read_vector(connection, api_version, reducer):
    if api_version < ComparableVersion("1.0.0"):
        pytest.skip()
    res = (
        connection.load_collection("S2")
            .filter_bbox(3, 6, 52, 50, "EPSG:4326")
            .aggregate_spatial(geometries="/some/path/to/GeometryCollection.geojson", reducer=reducer)
    )
    assert get_execute_graph(res) == load_json_resource('data/%s/aggregate_zonal_path.json' % api_version)

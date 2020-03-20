import shapely.geometry

from .. import get_execute_graph
from ... import load_json_resource


def test_polygon_timeseries_polygon(connection, api_version):
    polygon = shapely.geometry.shape(load_json_resource("data/polygon.json"))
    fapar = (
        connection
            .load_collection("S2")
            .filter_bbox(3, 6, 52, 50, "EPSG:4326")
            .polygonal_mean_timeseries(polygon)
    )
    assert get_execute_graph(fapar) == load_json_resource('data/%s/aggregate_zonal_polygon.json' % api_version)


def test_polygon_timeseries_path(connection, api_version):
    probav_s10_toc_ndvi = (
        connection.load_collection('S2')
            .bbox_filter(west=3, east=6, north=52, south=50, crs='EPSG:4326')
            .polygonal_mean_timeseries(polygon="/some/path/to/GeometryCollection.geojson")
    )
    actual = get_execute_graph(probav_s10_toc_ndvi)
    assert actual == load_json_resource('data/%s/aggregate_zonal_path.json' % api_version)

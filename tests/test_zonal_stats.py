from unittest import TestCase

import requests_mock
from shapely.geometry import shape

import openeo
from . import load_json_resource


@requests_mock.mock()
class TestTimeSeries(TestCase):

    def test_polygon_timeseries(self, m):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.connect(url="http://localhost:8000/api")
        #session.post = MagicMock()
        #session.download = MagicMock()

        m.get("http://localhost:8000/api/", json={"version": "0.4.0"})
        m.get("http://localhost:8000/api/collections", json=[{"product_id": "sentinel2_subset"}])
        m.get("http://localhost:8000/api/collections/SENTINEL2_FAPAR", json={"product_id": "sentinel2_subset",
                                                                               "bands": [{'band_id': 'FAPAR'}],
                                                                               })

        #discovery phase: find available data
        #basically user needs to find available data on a website anyway?
        #like the imagecollection ID on: https://earthengine.google.com/datasets/

        #access multiband 4D (x/y/time/band) coverage
        fapar = session.imagecollection("SENTINEL2_FAPAR").filter_bbox(3,6,52,50,"EPSG:4326")


        def check_process_graph(request):
            expected_graph = load_json_resource('data/aggregate_zonal.json')
            assert request.json() == expected_graph
            return True

        m.post("http://localhost:8000/api/result", json={}, additional_matcher=check_process_graph)

        polygon = load_json_resource("data/polygon.json")
        fapar.polygonal_mean_timeseries(shape(polygon)).execute()

        #get result as timeseries for a single point
        #How to define a point? Ideally it should also have the CRS?

    def test_polygon_timeseries_from_vector_file(self, m):
        session = openeo.connect(url="http://localhost:8000/api")

        m.get("http://localhost:8000/api/", json={"version": "0.4.0"})
        m.get("http://localhost:8000/api/collections/SENTINEL2_FAPAR", json={
            "product_id": "SENTINEL2_FAPAR"
        })

        probav_s10_toc_ndvi = session.imagecollection('SENTINEL2_FAPAR') \
            .bbox_filter(west=3, east=6, north=52, south=50, crs='EPSG:4326')

        def check_process_graph(request):
            expected_graph = load_json_resource('data/aggregate_zonal_vector_file.json')
            assert request.json() == expected_graph
            return True

        m.post("http://localhost:8000/api/result", json={}, additional_matcher=check_process_graph)

        polygon = "/data/users/Public/vdboschj/EP-3025/GeometryCollection.geojson"
        probav_s10_toc_ndvi.polygonal_mean_timeseries(polygon).execute()

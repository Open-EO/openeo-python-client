import os
import unittest
from unittest import TestCase

from mock import MagicMock
import requests_mock
from pathlib import Path

from shapely.geometry import shape

import openeo

def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)


def load_json(relative_path):
    import json
    with open(get_test_resource(relative_path), 'r+') as f:
        return json.load(f)

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
                                                                               'time': {'from': '2015-06-23', 'to': '2018-06-18'}})

        #discovery phase: find available data
        #basically user needs to find available data on a website anyway?
        #like the imagecollection ID on: https://earthengine.google.com/datasets/

        #access multiband 4D (x/y/time/band) coverage
        fapar = session.imagecollection("SENTINEL2_FAPAR").bbox_filter(3,6,52,50,"EPSG:4326")


        def check_process_graph(request):
            expected_graph = load_json('aggregate_zonal.json')
            assert request.json() == expected_graph
            return True

        m.post("http://localhost:8000/api/result", json={}, additional_matcher=check_process_graph)

        polygon = load_json("polygon.json")
        fapar.polygonal_mean_timeseries(shape(polygon)).execute()

        #get result as timeseries for a single point
        #How to define a point? Ideally it should also have the CRS?




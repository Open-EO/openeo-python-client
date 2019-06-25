import os
import unittest
from unittest import TestCase

from mock import MagicMock
import requests_mock
from pathlib import Path

import openeo


def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)


def load_json_resource(relative_path):
    import json
    with open(get_test_resource(relative_path), 'r+') as f:
        return json.load(f)


@requests_mock.mock()
class TestLogicalOps(TestCase):

    def test_not_equal(self,m):
        # configuration phase: define username, endpoint, parameters?
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        m.get("http://localhost:8000/api/", json={"version": "0.4.0"})
        m.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/SENTINEL2_SCF", json={"product_id": "sentinel2_subset",
                                                                                      "bands": [{'band_id': 'SCENECLASSIFICATION'}],
                                                                                      'time': {'from': '2015-06-23','to': '2018-06-18'}})

        # discovery phase: find available data
        # basically user needs to find available data on a website anyway?
        # like the imagecollection ID on: https://earthengine.google.com/datasets/

        # access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_SCF")

        scf_bands = s2_radio.band('SCENECLASSIFICATION')

        mask = scf_bands !=4

        mask.download("out.geotiff", bbox="", time=s2_radio.dates['to'])

        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        import json
        print(json.dumps(actual_graph,indent=2))
        expected_graph = load_json_resource('notequal.json')
        self.assertDictEqual(expected_graph,actual_graph)

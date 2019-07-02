from unittest import TestCase

import requests_mock
from mock import MagicMock

import openeo
from tests import load_json_resource


@requests_mock.mock()
class TestTemporal(TestCase):

    def test_apply_dimension_temporal_cumsum(self,m):
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        m.get("http://localhost:8000/api/", json={"version": "0.4.0"})
        m.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                                      "bands": [{'band_id': 'B02'},
                                                                                                {'band_id': 'B04'},
                                                                                                {'band_id': 'B08'},
                                                                                                ],
                                                                                      'time': {'from': '2015-06-23',
                                                                                               'to': '2018-06-18'}})

        # discovery phase: find available data
        # basically user needs to find available data on a website anyway?
        # like the imagecollection ID on: https://earthengine.google.com/datasets/

        # access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")
        s2_radio = s2_radio.apply_dimension('cumsum')

        s2_radio.download("out.geotiff", bbox="", time='2018-06-18')

        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        expected_graph = load_json_resource('data/apply_dimension_temporal_cumsum.json')
        assert actual_graph == expected_graph

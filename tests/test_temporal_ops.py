from unittest import TestCase

import requests_mock
from mock import MagicMock
import pytest

from openeo.internal.graphbuilder_040 import GraphBuilder as GraphBuilder040
from openeo.internal.graphbuilder import GraphBuilder as GraphBuilder100
import openeo
from . import load_json_resource

@pytest.fixture(scope="module", params=["0.4.0", "1.0.0"])
def version(request):
    return request.param


class TestTemporal():

    @pytest.fixture(autouse=True)
    def setup(self, version):
        self.version = version
        GraphBuilder040.id_counter = {}

    def test_apply_dimension_temporal_cumsum(self,requests_mock):
        requests_mock.get("http://localhost:8000/api/", json={"version": self.version})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                                      "bands": [{'band_id': 'B02'},
                                                                                                {'band_id': 'B04'},
                                                                                                {'band_id': 'B08'},
                                                                                                ],
                                                                                    })

        # discovery phase: find available data
        # basically user needs to find available data on a website anyway?
        # like the imagecollection ID on: https://earthengine.google.com/datasets/

        # access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")
        s2_radio = s2_radio.apply_dimension('cumsum')

        s2_radio.download("out.geotiff", format="GTIFF")

        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        expected_graph = load_json_resource('data/%s/apply_dimension_temporal_cumsum.json'%self.version)
        assert actual_graph == expected_graph

from unittest import TestCase

import pytest
import requests_mock
from mock import MagicMock

import openeo
from openeo import ImageCollection
from openeo.graphbuilder import GraphBuilder
from openeo.graphbuilder_100 import GraphBuilder as GraphBuilder100
from . import load_json_resource

@pytest.fixture(scope="module", params=["0.4.0", "1.0.0"])
def version(request):
    return request.param


#@requests_mock.mock()
class TestLogicalOps():

    @pytest.fixture(autouse=True)
    def setup(self,version):
        self.version = version
        GraphBuilder.id_counter = {}
        GraphBuilder100.id_counter = {}


    def test_not_equal(self, requests_mock):
        # configuration phase: define username, endpoint, parameters?
        requests_mock.get("http://localhost:8000/api/", json={"api_version":self.version})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_SCF", json={
            "product_id": "sentinel2_subset",
            "bands": [{'band_id': 'SCENECLASSIFICATION'}],
        })

        # discovery phase: find available data
        # basically user needs to find available data on a website anyway?
        # like the imagecollection ID on: https://earthengine.google.com/datasets/

        # access multiband 4D (x/y/time/band) coverage
        s2_radio = session.load_collection("SENTINEL2_SCF")

        scf_bands = s2_radio.band('SCENECLASSIFICATION')

        mask = scf_bands != 4

        mask.download("out.geotiff", format="GTIFF")

        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        expected_graph = load_json_resource('data/notequal.json')
        assert actual_graph == expected_graph

    def test_or(self, requests_mock):
        requests_mock.get("http://localhost:8000/api/", json={"api_version": self.version})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_SCF", json={
            "product_id": "sentinel2_subset",
            "bands": [{'band_id': 'SCENECLASSIFICATION'}],
        })

        ic = session.imagecollection("SENTINEL2_SCF")
        data = ic.band('SCENECLASSIFICATION')
        # TODO: this expression (twice `data`) creates a duplicate `arrayelement` in the process graph
        mask = (data == 2) | (data == 5)

        mask.download("out.geotiff", format="GTIFF")
        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        expected_graph = load_json_resource('logical_or.json')
        assert actual_graph == expected_graph

    def test_and(self, requests_mock):
        requests_mock.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_SCF", json={
            "product_id": "sentinel2_subset",
            "bands": [{'band_id': 'B1'}, {'band_id': 'B2'}],
        })

        ic = session.imagecollection("SENTINEL2_SCF")
        b1 = ic.band('B1')
        b2 = ic.band('B2')
        mask = (b1 == 2) & (b2 == 5)

        mask.download("out.geotiff", format="GTIFF")
        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        expected_graph = load_json_resource('logical_and.json')
        assert actual_graph == expected_graph

    def test_merging_cubes(self,requests_mock):
        requests_mock.get("http://localhost:8000/api/", json={"version": self.version})
        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_SCF", json={
            "product_id": "sentinel2_subset",
            "bands": [{'band_id': 'B1'}, {'band_id': 'B2'}],
        })

        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        ic = session.imagecollection("SENTINEL2_SCF")
        b1 = ic.band('B1') > 1
        b2 = ic.band('B2') > 2
        b1 = b1.linear_scale_range(0,1,0,2)
        b2 = b2.linear_scale_range(0, 1, 0, 2)

        combined = b1 | b2

        combined.download("out.geotiff", format="GTIFF")
        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]

        expected_graph = load_json_resource('data/%s/cube_merge_or.json'%self.version)
        assert actual_graph == expected_graph



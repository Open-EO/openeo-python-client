# -*- coding: utf-8 -*-
import json
import os
import tempfile
import unittest
from unittest import TestCase

import pytest
import requests_mock

import openeo
from openeo.capabilities import ApiVersionException


# MockUp Testdata
COLLECTIONS = [{'product_id': 'ASTER/AST_L1T_003',
  'description': 'ASTER L1T Radiance',
  'source': 'NASA LP DAAC at the USGS EROS Center, https://lpdaac.usgs.gov/dataset_discovery/aster/aster_products_table/ast_l1t'},
 {'product_id': 'AU/GA/AUSTRALIA_5M_DEM',
  'description': 'Australian 5M DEM',
  'source': 'Geoscience Australia, https://ecat.ga.gov.au/geonetwork/srv/eng/search#!22be4b55-2465-4320-e053-10a3070a5236'},
 {'product_id': 'COPERNICUS/S2',
 'description': 'Sentinel-2 MSI: MultiSpectral Instrument, Level-1C',
 'source': 'European Union/ESA/Copernicus, https://sentinel.esa.int/web/sentinel/user-guides/sentinel-2-msi',
 'bands': [{'band_id': 'B1'},
  {'band_id': 'B2'},
  {'band_id': 'B3'},
  {'band_id': 'B4'},
  {'band_id': 'B5'},
  {'band_id': 'B6'},
  {'band_id': 'B7'},
  {'band_id': 'B8'},
  {'band_id': 'B8A'},
  {'band_id': 'B9'},
  {'band_id': 'B10'},
  {'band_id': 'B11'},
  {'band_id': 'B12'},
  {'band_id': 'QA10'},
  {'band_id': 'QA20'},
  {'band_id': 'QA60'}],
 'extent': {'srs': 'EPSG:4326',
  'left': -180,
  'right': 180,
  'bottom': -90,
  'top': 90}}]

PROCESSES = [{'process_id': 'zonal_statistics',
  'description': 'Calculates statistics for each zone specified in a file.'},
 {'process_id': 'NDVI',
  'description': 'Finds the minimum value of time series for all bands of the input dataset.'},
 {'process_id': 'filter_bands',
  'description': 'Selects certain bands from a collection.'}]


@requests_mock.mock()
class TestUserFiles(TestCase):

    def setUp(self):
        # configuration phase: define username, endpoint, parameters?
        self.endpoint = "http://localhost:8000/api"
        self.user_id = "174367998144"
        self.auth_id = "test"
        self.auth_pwd = "test"
        self.upload_remote_fname = 'polygon.json'
        self.upload_local_fname = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                               'polygon.json')

    def match_uploaded_file(self, request):

        with open(self.upload_local_fname, 'r') as uploaded_file:
            content = uploaded_file.read()
        assert request.json() == json.loads(content)
        return True

    def match_process_graph(self, request):

        assert request.json() == PROCESSES
        return True

    @unittest.skip("Not yet upgraded to version 0.3.1")
    def test_user_upload_file(self, m):
        upload_url = "{}/files/{}/{}".format(self.endpoint, self.user_id, self.upload_remote_fname)
        m.register_uri('PUT', upload_url, additional_matcher=self.match_uploaded_file)
        session = openeo.session(self.user_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        status = session.user_upload_file(self.upload_local_fname,
                                          remote_path=self.upload_remote_fname)
        assert status

    @unittest.skip("Not yet upgraded to version 0.3.1")
    def test_user_download_file(self, m):
        download_url = "{}/users/{}/files/{}".format(self.endpoint, self.user_id, self.upload_remote_fname)
        with open(self.upload_local_fname, 'rb') as response_file:
            content = response_file.read()
        m.get(download_url, content=content)
        m.get("{}/auth/login".format(self.endpoint))
        connection = openeo.connect(self.endpoint,auth_options={'username':self.auth_id,'password':self.auth_pwd})

        local_output_fd, local_output_fname = tempfile.mkstemp()
        try:
            status = connection.user_download_file(self.upload_remote_fname,local_output_fname)
            assert status
            with open(local_output_fname, 'rb') as downloaded_file:
                downloaded_content = downloaded_file.read()
        finally:
            os.close(local_output_fd)
            os.remove(local_output_fname)

        assert content == downloaded_content

    @unittest.skip("Not yet upgraded to version 0.3.1")
    def test_user_delete_file(self, m):
        delete_url = "{}/users/{}/files/{}".format(self.endpoint, self.user_id,
                                                                         self.upload_remote_fname)
        m.register_uri('DELETE', delete_url)
        session = openeo.session(self.user_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        status = session.user_delete_file(self.upload_remote_fname)
        assert status

    def test_list_capabilities(self, m):
        capabilities = {
            "api_version": "0.4.0",
            "endpoints": [
                {"path": "/collections", "methods": ["GET"]},
            ]
        }
        m.get("{}/".format(self.endpoint), json=capabilities)
        con = openeo.connect(self.endpoint)
        res = con.capabilities()
        assert res.capabilities == capabilities

    def test_capabilities_api_version_too_old(self, m):
        m.register_uri('GET', "{}/".format(self.endpoint), json={'version': '0.3.1'})
        with pytest.raises(ApiVersionException):
            openeo.connect(self.endpoint)

    def test_capabilities_api_version_too_old2(self, m):
        m.register_uri('GET', "{}/".format(self.endpoint), json={'api_version': '0.3.1'})
        with pytest.raises(ApiVersionException):
            openeo.connect(self.endpoint)

    def test_capabilities_api_version_recent(self, m):
        m.register_uri('GET', "{}/".format(self.endpoint), json={'version': '0.4.0'})
        capabilities = openeo.connect(self.endpoint).capabilities()
        assert capabilities.version() == '0.4.0'
        assert capabilities.api_version() == '0.4.0'

    def test_capabilities_api_version_recent2(self, m):
        m.register_uri('GET', "{}/".format(self.endpoint), json={'api_version': '0.4.1'})
        capabilities = openeo.connect(self.endpoint).capabilities()
        assert capabilities.version() == '0.4.1'
        assert capabilities.api_version() == '0.4.1'

    def test_capabilities_api_version_check(self, m):
        capabilties_url = "{}/".format(self.endpoint)
        m.register_uri('GET', capabilties_url, json={'api_version': '1.2.3'})
        capabilities = openeo.connect(self.endpoint).capabilities()
        assert capabilities.api_version_check.below('1.2.4')
        assert capabilities.api_version_check.below('1.1') is False
        assert capabilities.api_version_check.at_most('1.3')
        assert capabilities.api_version_check.at_most('1.2.3')
        assert capabilities.api_version_check.at_most('1.2.2') is False
        assert capabilities.api_version_check.at_least('1.2.3')
        assert capabilities.api_version_check.at_least('1.5') is False
        assert capabilities.api_version_check.above('1.2.3') is False
        assert capabilities.api_version_check.above('1.2.2')

    def test_list_collections(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        con = openeo.connect(self.endpoint)

        collection_url = "{}/collections".format(self.endpoint)
        m.register_uri('GET', collection_url, json={'collections': COLLECTIONS})
        collections = con.list_collections()
        assert collections == COLLECTIONS

    def test_get_collection(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        con = openeo.connect(self.endpoint)

        collection_org = COLLECTIONS[0]
        collection_id = collection_org["product_id"]
        collection_url = "{}/collections/{}".format(self.endpoint, collection_id)
        m.register_uri('GET', collection_url, json=collection_org)
        collection = con.describe_collection(collection_id)
        assert collection == collection_org

    def test_get_all_processes(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        con = openeo.connect(self.endpoint)

        processes_url = "{}/processes".format(self.endpoint)
        m.register_uri('GET', processes_url, json={"processes": PROCESSES})
        processes = con.list_processes()
        assert processes == PROCESSES

    @unittest.skip("Not yet upgraded to version 0.3.1")
    def test_create_job(self, m):

        post_data = PROCESSES
        job_id = "MyId"
        result = {"job_id": job_id}

        m.register_uri('POST', "{}/jobs".format(self.endpoint), status_code=200, json=result) #additional_matcher=self.match_process_graph)
        #m.register_uri('POST', "{}/jobs".format(self.endpoint), status_code=400, additional_matcher=self.match_process_graph)

        con = openeo.connect(self.endpoint)

        resp = con.create_job(post_data)

        assert resp == job_id

        resp = con.create_job(post_data, evaluation="wrong")

        assert resp is None

    @unittest.skip("Not yet upgraded to version 0.3.1")
    def test_image(self, m):

        collection_org = COLLECTIONS[2]
        collection_id = collection_org["product_id"]
        collection_url = "{}/data/{}".format(self.endpoint, collection_id)
        m.register_uri('GET', collection_url, json=collection_org)

        con = openeo.connect(self.endpoint)

        resp = con.get_collection(collection_id)

        assert resp.bands == ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8',
                             'B8A', 'B9', 'B10', 'B11', 'B12', 'QA10', 'QA20', 'QA60']

    @unittest.skip("Not yet upgraded to version 0.3.1")
    def test_viewing_service(self, m):

        collection_org = COLLECTIONS[2]
        collection_id = collection_org["product_id"]
        collection_url = "{}/data/{}".format(self.endpoint, collection_id)

        def match_graph(request):
            self.assertDictEqual({
                "custom_param":45,
                "process_graph":{'product_id': 'COPERNICUS/S2'},
                "type":'WMTS',
                "title":"My Service",
                "description":"Service description"
            },request.json())
            return True

        m.get(collection_url,json=collection_org)
        m.post("http://localhost:8000/api/services",json={},additional_matcher=match_graph)

        session = openeo.session(self.user_id, endpoint=self.endpoint)

        resp = session.image(collection_id)
        resp.tiled_viewing_service(type="WMTS",title = "My Service", description = "Service description",custom_param=45)

    @unittest.skip("Not yet upgraded to version 0.3.1")
    def user_jobs(self, m):

        collection_url = "{}/users/{}/jobs".format(self.endpoint, self.user_id)
        m.register_uri('GET', collection_url, json=PROCESSES)

        session = openeo.session(self.user_id, endpoint=self.endpoint)

        resp = session.user_jobs()

        assert resp == PROCESSES

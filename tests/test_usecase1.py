import unittest
from unittest import TestCase

import requests_mock

import openeo
from openeo.rest.auth.auth import BearerAuth

POST_DATA = '{"process_id": "filter_daterange", "args": { "imagery": { "product_id": "landsat7_ndvi"}, ' \
            '"from": "2014-01-01", "to": "2014-06-01"}}'


@requests_mock.mock()
class TestUsecase1(TestCase):

    def setUp(self):
        # configuration phase: define username, endpoint, parameters?
        self.endpoint = "http://localhost:8000/api"
        self.user_id = "174367998144"
        self.auth_id = "test"
        self.auth_pwd = "test"

        self.data_id= "sentinel2_subset"
        self.process_id = "calculate_ndvi"
        self.output_file = "/tmp/test.gtiff"

    def test_user_login(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        m.get("http://localhost:8000/api/credentials/basic", json={"access_token": "blabla"})
        con = openeo.connect(self.endpoint).authenticate_basic(username=self.auth_id, password=self.auth_pwd)
        assert isinstance(con.auth, BearerAuth)

    def test_viewing_userjobs(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        m.get("http://localhost:8000/api/credentials/basic", json={"access_token": "blabla"})
        m.get("http://localhost:8000/api/jobs", json={"jobs": [{"job_id": "748df7caa8c84a7ff6e"}]})
        con = openeo.connect(self.endpoint).authenticate_basic(username=self.auth_id, password=self.auth_pwd)
        userjobs = con.list_jobs()
        self.assertGreater(len(userjobs), 0)

    def test_viewing_data(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        m.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/sentinel2_subset", json={"product_id": "sentinel2_subset"})

        con = openeo.connect(self.endpoint)
        data = con.list_collections()

        self.assertGreater(str(data).find(self.data_id), -1)

        data_info = con.describe_collection(self.data_id)

        self.assertEqual(data_info["product_id"], self.data_id)

    def test_viewing_processes(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        con = openeo.connect(self.endpoint)

        m.get("http://localhost:8000/api/processes", json={"processes": [{"process_id": "calculate_ndvi"}]})
        processes = con.list_processes()
        assert self.process_id in set(p["process_id"] for p in processes)

    def test_job_creation(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        m.get("http://localhost:8000/api/credentials/basic", json={"access_token": "blabla"})
        m.post("http://localhost:8000/api/jobs", status_code=201,headers={"OpenEO-Identifier": "748df7caa8c84a7ff6e"})

        con = openeo.connect(self.endpoint).authenticate_basic(self.auth_id, self.auth_pwd)

        job_id = con.create_job(POST_DATA)
        self.assertIsNotNone(job_id)


if __name__ == '__main__':
    unittest.main()

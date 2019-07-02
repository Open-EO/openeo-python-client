import unittest
from unittest import TestCase
import os
from unittest.mock import MagicMock

import openeo
import requests_mock

POST_DATA = '{"process_id": "filter_daterange", "args": { "imagery": { "product_id": "landsat7_ndvi"}, ' \
            '"from": "2014-01-01", "to": "2014-06-01"}}'


@requests_mock.mock()
class TestUsecase1(TestCase):

    def setUp(self):
        # configuration phase: define username, endpoint, parameters?
        self.endpoint = "http://localhost:8000/api"
        self.uiser_id = "174367998144"
        self.auth_id = "test"
        self.auth_pwd = "test"

        self.data_id= "sentinel2_subset"
        self.process_id = "calculate_ndvi"
        self.output_file = "/tmp/test.gtiff"

    def test_user_login(self, m):
        m.get("http://localhost:8000/api/credentials/basic", json={"token": "blabla"})
        con = openeo.connect(self.endpoint, auth_options={"username": self.auth_id, "password": self.auth_pwd})

        self.assertNotEqual(con, None)

    def test_viewing_userjobs(self, m):
        m.get("http://localhost:8000/api/credentials/basic", json={"token": "blabla"})
        m.get("http://localhost:8000/api/jobs", json=[{"job_id": "748df7caa8c84a7ff6e"}])

        con = openeo.connect(self.endpoint, auth_options={"username": self.auth_id, "password": self.auth_pwd})

        userjobs = con.list_jobs()

        self.assertGreater(len(userjobs), 0)

    def test_viewing_data(self, m):
        m.get("http://localhost:8000/api/collections", json=[{"product_id": "sentinel2_subset"}])
        m.get("http://localhost:8000/api/collections/sentinel2_subset", json={"product_id": "sentinel2_subset"})

        con = openeo.connect(self.endpoint)
        data = con.list_collections()

        self.assertGreater(str(data).find(self.data_id), -1)

        data_info = con.describe_collection(self.data_id)

        self.assertEqual(data_info["product_id"], self.data_id)

    def test_viewing_processes(self, m):
        m.get("http://localhost:8000/api/processes", json=[{"process_id": "calculate_ndvi"}])

        con = openeo.connect(self.endpoint)
        processes = con.list_processes()

        self.assertGreater(str(processes).find(self.process_id), -1)

    def test_job_creation(self, m):
        m.get("http://localhost:8000/api/credentials/basic", json={"token": "blabla"})
        m.post("http://localhost:8000/api/jobs", status_code=201,headers={"OpenEO-Identifier": "748df7caa8c84a7ff6e"})

        con = openeo.connect(self.endpoint, auth_options={"username": self.auth_id, "password": self.auth_pwd})

        job_id = con.create_job(POST_DATA)
        self.assertIsNotNone(job_id)


if __name__ == '__main__':
    unittest.main()

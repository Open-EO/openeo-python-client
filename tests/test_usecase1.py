import unittest
from unittest import TestCase

import requests_mock

import openeo
from openeo.rest.auth.auth import BearerAuth


@requests_mock.mock()
class TestUsecase1(TestCase):
    _capabilities = {
        "api_version": "1.0.0",
        "endpoints": [{"path": "/credentials/basic", "methods": ["GET"]}],
    }

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
        m.get("http://localhost:8000/api/", json=self._capabilities)
        m.get("http://localhost:8000/api/credentials/basic", json={"access_token": "blabla"})
        con = openeo.connect(self.endpoint).authenticate_basic(username=self.auth_id, password=self.auth_pwd)
        assert isinstance(con.auth, BearerAuth)

    def test_viewing_list_jobs(self, m):
        m.get("http://localhost:8000/api/", json=self._capabilities)
        m.get("http://localhost:8000/api/credentials/basic", json={"access_token": "blabla"})
        job_info = {"job_id": "748df7caa8c84a7ff6e", "status": "running", "created": "2021-02-22T09:00:00Z"}
        m.get("http://localhost:8000/api/jobs", json={"jobs": [job_info]})
        con = openeo.connect(self.endpoint).authenticate_basic(username=self.auth_id, password=self.auth_pwd)
        jobs = con.list_jobs()
        assert jobs == [job_info]

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
        m.get("http://localhost:8000/api/", json=self._capabilities)
        m.get("http://localhost:8000/api/credentials/basic", json={"access_token": "blabla"})
        m.post("http://localhost:8000/api/jobs", status_code=201,headers={"OpenEO-Identifier": "748df7caa8c84a7ff6e"})

        con = openeo.connect(self.endpoint).authenticate_basic(self.auth_id, self.auth_pwd)

        pg = {"add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        job = con.create_job(pg)
        assert job.job_id == "748df7caa8c84a7ff6e"


if __name__ == '__main__':
    unittest.main()

import unittest
from unittest import TestCase
import os
from unittest.mock import MagicMock

import openeo
import requests_mock

POST_DATA = '{"process_id": "filter_daterange", "args": { "imagery": { "product_id": "landsat7_ndvi"}, "from": "2014-01-01", "to": "2014-06-01"}}'


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
        self.output_file = "/home/berni/test.gtiff"

    def test_user_login(self, m):
        m.get("http://localhost:8000/api/auth/login", json={"token": "blabla"})

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        token = session.auth(self.auth_id, self.auth_pwd)
        self.assertNotEqual(token, None)

    def test_viewing_userjobs(self, m):
        m.get("http://localhost:8000/api/auth/login", json={"token": "blabla"})
        m.get("http://localhost:8000/api/users/%s/jobs" % self.uiser_id, json=[{"job_id": "748df7caa8c84a7ff6e"}])

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        userjobs = session.user_jobs()

        self.assertGreater(len(userjobs), 0)

    def test_viewing_data(self, m):
        m.get("http://localhost:8000/api/data", json=[{"product_id": "sentinel2_subset"}])
        m.get("http://localhost:8000/api/data/sentinel2_subset", json={"product_id": "sentinel2_subset"})

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        data = session.list_collections()

        self.assertGreater(str(data).find(self.data_id), -1)

        data_info = session.get_collection(self.data_id)

        self.assertEqual(data_info["product_id"], self.data_id)

    def test_viewing_processes(self, m):
        m.get("http://localhost:8000/api/processes", json=[{"process_id": "calculate_ndvi"}])
        m.get("http://localhost:8000/api/processes/calculate_ndvi", json={"process_id": "calculate_ndvi"})

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        processes = session.get_all_processes()

        self.assertGreater(str(processes).find(self.process_id), -1)

        process_info = session.get_process(self.process_id)

        self.assertEqual(process_info["process_id"], self.process_id)

    def test_job_creation(self, m):
        m.get("http://localhost:8000/api/auth/login", json={"token": "blabla"})
        m.post("http://localhost:8000/api/jobs?evaluate=lazy", json={"job_id": "748df7caa8c84a7ff6e"})

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)

        job_id = session.create_job(POST_DATA)
        self.assertIsNotNone(job_id)

        #session.download_image(job_id, self.output_file,"image/gtiff")

       # self.assertTrue(os.path.isfile(self.output_file))


if __name__ == '__main__':
    unittest.main()

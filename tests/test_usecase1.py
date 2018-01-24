import unittest
from unittest import TestCase
import os
from unittest.mock import MagicMock

import openeo

POST_DATA = '{"process_id": "filter_daterange", "args": { "imagery": { "product_id": "landsat7_ndvi"}, "from": "2014-01-01", "to": "2014-06-01"}}'


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


    def test_user_login(self):

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        token = session.auth(self.auth_id, self.auth_pwd)
        self.assertNotEqual(token, None)

    def test_viewing_userjobs(self):

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        userjobs = session.user_jobs()

        self.assertGreater(len(userjobs), 0)

    def test_viewing_data(self):

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        data = session.get_all_data()

        self.assertGreater(str(data).find(self.data_id), -1)

        data_info = session.get_data(self.data_id)

        self.assertEqual(data_info["product_id"], self.data_id)

    def test_viewing_processes(self):

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        processes = session.get_all_processes()

        self.assertGreater(str(processes).find(self.process_id), -1)

        process_info = session.get_process(self.process_id)

        self.assertEqual(process_info["process_id"], self.process_id)

    def test_job_creation(self):

        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        job_id = session.create_job(POST_DATA)
        self.assertIsNotNone(job_id)

        #session.download_image(job_id, self.output_file,"image/gtiff")

       # self.assertTrue(os.path.isfile(self.output_file))


if __name__ == '__main__':
    unittest.main()
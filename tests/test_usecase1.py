import unittest
from unittest import TestCase

from unittest.mock import MagicMock

import openeo


class TestUsecase1(TestCase):

    def setUp(self):
        self.endpoint = "http://localhost:8000/api"
        self.uiser_id = "174367998144"
        self.auth_id = "test"
        self.auth_pwd = "test"

        self.data_id= "sentinel2_subset"


    def test_user_login(self):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        token = session.auth(self.auth_id, self.auth_pwd)
        self.assertNotEqual(token, None)

    def test_viewing_userjobs(self):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        userjobs = session.user_jobs()

        self.assertGreater(len(userjobs), 0)

    def test_viewing_data(self):
        #configuration phase: define username, endpoint, parameters?


        session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        data = session.get_all_data()

        self.assertGreater(str(data).find(self.data_id), -1)

        data_info = session.get_data(self.data_id)

        self.assertEqual(data_info["product_id"],self.data_id)

        

if __name__ == '__main__':
    unittest.main()
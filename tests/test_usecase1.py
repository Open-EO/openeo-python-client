import unittest
from unittest import TestCase

from unittest.mock import MagicMock

import openeo


class TestUsecase1(TestCase):


    def test_user_login(self):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.session("174367998144", endpoint="http://localhost:8000/api")
        token = session.auth("test", "test")
        self.assertNotEqual(token, None)

    def test_viewing_userjobs(self):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.session("174367998144", endpoint="http://localhost:8000/api")
        session.auth("test", "test")
        userjobs = session.user_jobs()
        userjobs = userjobs.text
        self.assertNotEqual(userjobs, "[]")


if __name__ == '__main__':
    unittest.main()
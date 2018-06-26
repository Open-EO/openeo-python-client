# -*- coding: utf-8 -*-

import openeo
from unittest import TestCase
import tempfile
import os
import json

import requests_mock


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

    def test_user_upload_file(self, m):
        upload_url ="http://localhost:8000/api/users/{}/files/{}".format(self.user_id,
                                                                         self.upload_remote_fname)
        m.register_uri('PUT', upload_url,
                       additional_matcher=self.match_uploaded_file)
        session = openeo.session(self.user_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        status = session.user_upload_file(self.upload_local_fname,
                                          remote_path=self.upload_remote_fname)
        assert status

    def test_user_download_file(self, m):
        download_url ="http://localhost:8000/api/users/{}/files/{}".format(self.user_id,
                                                                           self.upload_remote_fname)
        with open(self.upload_local_fname, 'rb') as response_file:
            content = response_file.read()
        m.get(download_url, content=content)
        session = openeo.session(self.user_id, endpoint=self.endpoint)
        session.auth(self.auth_id, self.auth_pwd)
        local_output_fname = tempfile.NamedTemporaryFile()
        status = session.user_download_file(self.upload_remote_fname,
                                            local_output_fname.name)
        assert status
        with open(local_output_fname.name, 'rb') as downloaded_file:
            downloaded_content = downloaded_file.read()
        assert content == downloaded_content

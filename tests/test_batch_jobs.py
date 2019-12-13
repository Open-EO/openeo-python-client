import unittest
from unittest import TestCase

import requests_mock
from mock import MagicMock

import openeo
from openeo.graphbuilder import GraphBuilder
from openeo import Job
from . import load_json_resource


@requests_mock.mock()
class TestBatchJobs(TestCase):

    def setUp(self) -> None:
        GraphBuilder.id_counter = {}

    def test_create_job(self, m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        m.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={})

        def match_body(request):
            assert request.json() == load_json_resource("data/batch_job.json")
            return True

        headers = {
            "OpenEO-Identifier": "my-identifier",
            "Location": "http://localhost:8000/api/jobs/my-identifier"
        }
        m.post("http://localhost:8000/api/jobs", status_code=201, headers=headers, additional_matcher=match_body)

        session = openeo.connect("http://localhost:8000/api")
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")
        job = s2_radio.send_job(out_format="GTIFF")
        assert job.job_id == "my-identifier"

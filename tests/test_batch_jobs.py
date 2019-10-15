import unittest
from unittest import TestCase

import requests_mock
from mock import MagicMock

import openeo
from openeo import Job
from . import load_json_resource


@requests_mock.mock()
class TestBatchJobs(TestCase):

    def test_create_job(self,m):
        m.get("http://localhost:8000/api/", json={"api_version": "0.4.0"})
        session = openeo.connect("http://localhost:8000/api")
        #session.post = MagicMock()
        session.download = MagicMock()

        m.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                                      "bands": [{'band_id': 'B02'},
                                                                                                {'band_id': 'B04'},
                                                                                                {'band_id': 'B08'},
                                                                                                ],
                                                                                    })

        def match_body(request):
            self.assertDictEqual(load_json_resource("data/batch_job.json"),request.json())
            return True

        m.post("http://localhost:8000/api/jobs",status_code=201,headers={"OpenEO-Identifier":"my-identifier","Location":"http://localhost:8000/api/jobs/my-identifier"},additional_matcher=match_body)
        # discovery phase: find available data
        # basically user needs to find available data on a website anyway?
        # like the imagecollection ID on: https://earthengine.google.com/datasets/

        # access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        job_info = s2_radio.send_job(out_format="GTIFF")
        self.assertEqual("my-identifier",job_info.job_id)


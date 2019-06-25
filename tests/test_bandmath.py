import os
import unittest
from unittest import TestCase

from mock import MagicMock
import requests_mock
from pathlib import Path

import openeo


def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)


def load_json_resource(relative_path):
    import json
    with open(get_test_resource(relative_path), 'r+') as f:
        return json.load(f)


@requests_mock.mock()
class TestBandMath(TestCase):

    def test_evi(self,m):
        # configuration phase: define username, endpoint, parameters?
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        m.get("http://localhost:8000/api/", json={"version": "0.4.0"})
        m.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                                      "bands": [{'band_id': 'B02'},
                                                                                                {'band_id': 'B04'},
                                                                                                {'band_id': 'B08'},
                                                                                                ],
                                                                                      'time': {'from': '2015-06-23',
                                                                                               'to': '2018-06-18'}})

        # discovery phase: find available data
        # basically user needs to find available data on a website anyway?
        # like the imagecollection ID on: https://earthengine.google.com/datasets/

        # access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        B02 = s2_radio.band('B02')
        B04 = s2_radio.band('B04')
        B08 = s2_radio.band('B08')

        evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)

        evi_cube.download("out.geotiff", bbox="", time=s2_radio.dates['to'])

        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        expected_graph = load_json_resource('evi_graph.json')
        assert actual_graph == expected_graph

    def test_ndvi(self, m):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        m.get("http://localhost:8000/api/", json={"version": "0.3.1"})
        m.get("http://localhost:8000/api/collections", json={"collections":[{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                               "bands": [{'band_id': 'B0'}, {'band_id': 'B1'},
                                                                                         {'band_id': 'B2'}, {'band_id': 'B3'}],
                                                                               'time': {'from': '2015-06-23', 'to': '2018-06-18'}})

        #discovery phase: find available data
        #basically user needs to find available data on a website anyway?
        #like the imagecollection ID on: https://earthengine.google.com/datasets/

        #access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        #how to find out which bands I need?
        #are band id's supposed to be consistent across endpoints? Is that possible?

        #define a computation to perform
        #combinebands to REST: udf_type:apply_pixel, lang: Python
        bandFunction = lambda band1, band2, band3: band1 + band2
        ndvi_coverage = s2_radio.apply_pixel([s2_radio.bands[0], s2_radio.bands[1], s2_radio.bands[2]], bandFunction)

        #materialize result in the shape of a geotiff
        #REST: WCS call
        ndvi_coverage.download("out.geotiff",bbox="", time=s2_radio.dates['to'])

        #get result as timeseries for a single point
        #How to define a point? Ideally it should also have the CRS?
        ndvi_coverage.timeseries(4, 51)

        import base64
        import cloudpickle
        expected_function = str(base64.b64encode(cloudpickle.dumps(bandFunction)),"UTF-8")
        expected_graph = {
            'process_graph': {
                'process_id': 'apply_pixel',
                'imagery':{
                        'name': 'SENTINEL2_RADIOMETRY_10M',
                        'process_id': 'get_collection'
                    },
                'bands': ['B0', 'B1', 'B2'],
                'function': expected_function

            }
        }
        session.post.assert_called_once_with("/timeseries/point?x=4&y=51&srs=EPSG:4326",expected_graph)
        session.download.assert_called_once()

    def test_ndvi_udf(self, m):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        m.get("http://localhost:8000/api/", json={"version": "0.3.1"})
        m.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                               "bands": [{'band_id': 'B0'},
                                                                                         {'band_id': 'B1'},
                                                                                         {'band_id': 'B2'},
                                                                                         {'band_id': 'B3'}],
                                                                               'time': {'from': '2015-06-23',
                                                                                        'to': '2018-06-18'}})

        #discovery phase: find available data
        #basically user needs to find available data on a website anyway?
        #like the imagecollection ID on: https://earthengine.google.com/datasets/

        #access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        #dir = os.path.dirname(openeo_udf.functions.__file__)
        #file_name = os.path.join(dir, "raster_collections_ndvi.py")
        #udf_code = UdfCode(language="python", source=open(file_name, "r").read())

        ndvi_coverage = s2_radio.apply_tiles( "def myfunction(tile): print(tile)")

        #materialize result in the shape of a geotiff
        #REST: WCS call
        ndvi_coverage.download("out.geotiff",bbox="", time=s2_radio.dates['to'])

        #get result as timeseries for a single point
        #How to define a point? Ideally it should also have the CRS?
        ndvi_coverage.timeseries(4, 51)

        expected_graph = {'process_graph': {'process_id': 'apply_tiles',
                                            'imagery': {'name': 'SENTINEL2_RADIOMETRY_10M', 'process_id': 'get_collection'},
                                             'code': {'language': 'python',
                                                      'source': 'def myfunction(tile): print(tile)'
                                                      }

                                            }
                          }
        session.post.assert_called_once_with("/timeseries/point?x=4&y=51&srs=EPSG:4326",expected_graph)
        session.download.assert_called_once()

    def test_ndvi_udf_0_4_0(self, m):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.connect("http://localhost:8000/api")

        m.get("http://localhost:8000/api/", json={"version": "0.4.0"})
        m.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        m.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                               "bands": [{'band_id': 'B0'},
                                                                                         {'band_id': 'B1'},
                                                                                         {'band_id': 'B2'},
                                                                                         {'band_id': 'B3'}],
                                                                               'time': {'from': '2015-06-23',
                                                                                        'to': '2018-06-18'}})

        def check_process_graph(request):
            expected_graph = load_json_resource('udf_graph.json')
            assert request.json() == expected_graph
            return True

        m.post("http://localhost:8000/api/result", text="my binary data", additional_matcher=check_process_graph)

        #access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        ndvi_coverage = s2_radio.apply_tiles( "def myfunction(tile):\n"
                                              "    print(tile)\n"
                                              "    return tile")

        #materialize result in the shape of a geotiff
        #REST: WCS call
        ndvi_coverage.download("out.geotiff",bbox="", time=s2_radio.dates['to'])

    @unittest.skip("Not yet implemented")
    def test_timeseries_fusion(self):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.session("driesj")

        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")
        probav_radio = session.imagecollection("PROBA_V_RADIOMETRY_100M")

        #we want to get data at fixed days of month, in roughly 10-daily periods,
        s2_radio.temporal_composite(method="day_of_month", resampling="nearest",days=[1,11,21])
        probav_radio.temporal_composite(method="day_of_month", resampling="nearest",days=[1,11,21])

        #how do pixels get aligned?
        #option 1: resample PROBA-V to S2
        probav_radio.resample(s2_radio.layout)

        #option 2: create lookup table/transformation that maps S2 pixel coordinate to corresponding PROBA-V pixel?
        #this does add a bunch of complexity, resampling is easier, but maybe requires more resources?

        #combine timeseries, assumes pixels are aligned?
        fused_timeseries = openeo.timeseries_combination([s2_radio,probav_radio])

        fused_timeseries.timeseries(4, 51)

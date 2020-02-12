import unittest
import pytest

import requests_mock
from mock import MagicMock

import openeo
from openeo.internal.graphbuilder_040 import GraphBuilder as GraphBuilder040
from openeo.internal.graphbuilder import GraphBuilder as GraphBuilder100
from . import load_json_resource

@pytest.fixture(scope="module", params=["0.4.0", "1.0.0"])
def version(request):
    return request.param

class TestBandMath():

    @pytest.fixture(autouse=True)
    def setup(self, version):
        self.version = version
        GraphBuilder040.id_counter = {}
        GraphBuilder100.id_counter = {}


    def test_basic(self, requests_mock):
        requests_mock.get("http://localhost:8000/api/", json={"api_version": self.version})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                                      "bands": [{'band_id': 'B02'},
                                                                                                {'band_id': 'B04'},
                                                                                                {'band_id': 'B08'},
                                                                                                ],
                                                                                      'time': {'from': '2015-06-23',
                                                                                               'to': '2018-06-18'}})

        cube = session.imagecollection("SENTINEL2_RADIOMETRY_10M")
        expected_graph = load_json_resource('data/%s/band0.json'%self.version)


        assert self._get_graph(cube.band(0)) == expected_graph
        GraphBuilder040.id_counter = {}
        GraphBuilder100.id_counter = {}
        assert self._get_graph(cube.band('B02')) == expected_graph

    def _get_graph(self,cube):
        if self.version == '0.4.0':
            return cube.graph
        else:
            return cube.builder.flatten()


    def test_band_indexing(self, requests_mock):
        self.maxDiff=None
        requests_mock.get("http://localhost:8000/api/", json={"api_version": self.version})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections/CGS_SENTINEL2_RADIOMETRY_V102_001", json={
            "id": "CGS_SENTINEL2_RADIOMETRY_V102_001",
            "properties": {
                "eo:bands": [
                    {"name": "B02", "common_name": "blue", "center_wavelength": 0.4966},
                    {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                    {"name": "B04", "common_name": "red", "center_wavelength": 0.6645},
                    {"name": "B08", "common_name": "nir", "center_wavelength": 0.8351}
                ]
            }
        })

        cube = session.imagecollection("CGS_SENTINEL2_RADIOMETRY_V102_001")
        expected_graph = load_json_resource('data/%s/band_red.json'%self.version)

        def check_cube(cube, band_index=2):
            GraphBuilder040.id_counter = {}
            GraphBuilder100.id_counter = {}
            assert self._get_graph(cube.band(band_index)) == expected_graph
            GraphBuilder040.id_counter = {}
            GraphBuilder100.id_counter = {}
            assert self._get_graph(cube.band('B04')) == expected_graph
            GraphBuilder040.id_counter = {}
            GraphBuilder100.id_counter = {}
            assert self._get_graph(cube.band('red')) == expected_graph
        check_cube(cube)

        expected_graph = load_json_resource('data/%s/band_red_filtered.json'%self.version)
        GraphBuilder040.id_counter = {}
        GraphBuilder100.id_counter = {}
        check_cube( cube.filter_bands(['red','green']),0)

    def test_evi(self,requests_mock):
        # configuration phase: define username, endpoint, parameters?
        requests_mock.get("http://localhost:8000/api/", json={"api_version": self.version})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                                      "bands": [{'band_id': 'B02'},
                                                                                                {'band_id': 'B04'},
                                                                                                {'band_id': 'B08'},
                                                                                                ],
                                                                                    })

        # discovery phase: find available data
        # basically user needs to find available data on a website anyway?
        # like the imagecollection ID on: https://earthengine.google.com/datasets/

        # access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        B02 = s2_radio.band('B02')
        B04 = s2_radio.band('B04')
        B08 = s2_radio.band('B08')

        evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)

        evi_cube.download("out.geotiff", format="GTIFF")

        session.download.assert_called_once()
        actual_graph = session.download.call_args_list[0][0][0]
        expected_graph = load_json_resource('data/%s/evi_graph.json'%self.version)
        assert actual_graph == expected_graph


    def test_ndvi_udf(self, requests_mock):
        #configuration phase: define username, endpoint, parameters?
        requests_mock.get("http://localhost:8000/api/", json={"version": "0.4.1"})
        session = openeo.connect("http://localhost:8000/api")
        session.post = MagicMock()
        session.download = MagicMock()

        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                               "bands": [{'band_id': 'B0'},
                                                                                         {'band_id': 'B1'},
                                                                                         {'band_id': 'B2'},
                                                                                         {'band_id': 'B3'}],
                                                                             })

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
        ndvi_coverage.download("out.geotiff", format="GTIFF")

        #get result as timeseries for a single point
        #How to define a point? Ideally it should also have the CRS?
        ndvi_coverage.execute()

        expected_graph = {
            'process_graph': {
                'loadcollection1': {
                    'result': False, 'process_id': 'load_collection',
                    'arguments': {'temporal_extent': None, 'id': 'SENTINEL2_RADIOMETRY_10M', 'spatial_extent': None}},
                'reduce1': {
                    'result': True, 'process_id': 'reduce',
                    'arguments': {
                        'reducer': {'callback': {
                            'udf': {'result': True, 'process_id': 'run_udf',
                                    'arguments': {'version': 'latest',
                                                  'udf': 'def myfunction(tile): print(tile)',
                                                  'runtime': 'Python',
                                                  'data': {'from_argument': 'data'}}}}},
                        'dimension': 'spectral_bands', 'binary': False,
                        'data': {'from_node': 'loadcollection1'}
                    }
                }
            }
        }

        session.post.assert_called_once_with(path="/result", json=expected_graph)
        session.download.assert_called_once()

    def test_ndvi_udf_0_4_0(self, requests_mock):
        #configuration phase: define username, endpoint, parameters?
        requests_mock.get("http://localhost:8000/api/", json={"api_version": self.version})
        session = openeo.connect("http://localhost:8000/api")

        requests_mock.get("http://localhost:8000/api/collections", json={"collections": [{"product_id": "sentinel2_subset"}]})
        requests_mock.get("http://localhost:8000/api/collections/SENTINEL2_RADIOMETRY_10M", json={"product_id": "sentinel2_subset",
                                                                               "bands": [{'band_id': 'B0'},
                                                                                         {'band_id': 'B1'},
                                                                                         {'band_id': 'B2'},
                                                                                         {'band_id': 'B3'}],
                                                                             })

        def check_process_graph(request):
            expected_graph = load_json_resource('data/%s/udf_graph.json'%self.version)
            import json
            print(json.dumps(request.json(),indent=2))
            assert request.json() == expected_graph
            return True

        requests_mock.post("http://localhost:8000/api/result", text="my binary data", additional_matcher=check_process_graph)

        #access multiband 4D (x/y/time/band) coverage
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        ndvi_coverage = s2_radio.apply_tiles( "def myfunction(tile):\n"
                                              "    print(tile)\n"
                                              "    return tile")

        #materialize result in the shape of a geotiff
        #REST: WCS call
        ndvi_coverage.download("out.geotiff", format="GTIFF")

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

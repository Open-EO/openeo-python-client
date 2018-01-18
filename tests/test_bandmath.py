import unittest
from unittest import TestCase

import openeo
from mock import MagicMock


class TestBandMath(TestCase):


    def test_ndvi(self):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.session("driesj",endpoint="https://myopeneo.be/")
        session.post = MagicMock()

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
        ndvi_coverage = s2_radio.combinebands([s2_radio.bands[0],s2_radio.bands[1],s2_radio.bands[2]], bandFunction)

        #materialize result in the shape of a geotiff
        #REST: WCS call
        ndvi_coverage.geotiff(bbox="",time=s2_radio.dates[0])

        #get result as timeseries for a single point
        #How to define a point? Ideally it should also have the CRS?
        ndvi_coverage.timeseries(4, 51)

        import base64
        import cloudpickle
        expected_function = str(base64.b64encode(cloudpickle.dumps(bandFunction)),"UTF-8")
        expected_graph = {
            'process_id': 'band_arithmetic',
            'args':
                {
                    'collections':[
                        {
                            'product_id': 'SENTINEL2_RADIOMETRY_10M'
                        }
                    ],
                    'bands': ['B0', 'B1', 'B2'],
                    'function': expected_function
                }
        }
        session.post.assert_called_once_with("/openeo/v0.1/timeseries/point?x=4&y=51&srs=EPSG:4326",expected_graph)



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
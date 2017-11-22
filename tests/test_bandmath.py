from unittest import TestCase

from dateutil.parser import parse
import openeo



class TestBandMath(TestCase):



    def test_ndvi(self):
        #configuration phase: define username, endpoint, parameters?
        session = openeo.session("driesj")

        #discovery phase: find available data
        #basically user needs to find available data on a website anyway?
        #like the imagecollection ID on: https://earthengine.google.com/datasets/

        #access multiband 4D (x/y/time/band) coverage
        coverage = session.coverage("SENTINEL2_RADIOMETRY_10M")

        #how to find out which bands I need?
        #are band id's supposed to be consistent across endpoints? Is that possible?

        #define a computation to perform
        #combinebands to REST: udf_type:apply_pixel, lang: Python
        ndvi_coverage = coverage.combinebands([coverage.bands[0],coverage.bands[1],coverage.bands[8]], lambda band1, band2, band3 : band1+band2 )

        #materialize result in the shape of a geotiff
        #REST: WCS call
        ndvi_coverage.geotiff(bbox="",time=coverage.dates[0])

        #get result as timeseries for a single point
        ndvi_coverage.meanseries(4,51)
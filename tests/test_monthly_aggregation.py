from unittest import TestCase
import openeo
from openeo.temporal import MONTH_OF_YEAR
from unittest import TestCase

from openeo.temporal import MONTH_OF_YEAR

import openeo


class TestMonthlyAggregation(TestCase):


    def test_monthly_aggregation(self):
        session = openeo.session("driesj")
        s2_radio = session.imagecollection("SENTINEL2_RADIOMETRY_10M")

        #how to find out which bands I need?
        #are band id's supposed to be consistent across endpoints? Is that possible?

        #define a computation to perform
        #combinebands to REST: udf_type:apply_pixel, lang: Python
        bandFunction = lambda timeseries: timeseries
        ndvi_coverage = s2_radio.reduceByTime(MONTH_OF_YEAR, bandFunction)
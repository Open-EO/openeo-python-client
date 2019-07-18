from shapely.geometry import Polygon

from openeo import ImageCollection

import openeo
import logging
import os
from pathlib import Path
import pandas as pd

#enable logging in requests library
from openeo.rest.imagecollectionclient import ImageCollectionClient

logging.basicConfig(level=logging.DEBUG)

#connect with EURAC backend
session = openeo.session("nobody", "http://openeo.vgt.vito.be/openeo/0.4.0")
#session = openeo.session("nobody", "http://localhost:5000/openeo/0.4.0")

#retrieve the list of available collections
collections = session.list_collections()
print(collections)

#create image collection


#specify process graph
#compute cloud mask

"""
cloud_mask = session.image("S2_SCENECLASSIFICATION") \
    .filter_temporal("2017-03-01","2017-02-20") \
    .bbox_filter(left=652000,right=672000,top=5161000,bottom=5181000,srs="EPSG:32632") \
    .apply_pixel(map_classification_to_binary)
"""

polygon = Polygon(shell= [
            [
              5.152158737182616,
              51.18469636040683
            ],
            [
              5.15183687210083,
              51.181979395425095
            ],
            [
              5.152802467346191,
              51.18192559252128
            ],
            [
              5.153381824493408,
              51.184588760878924
            ],
            [
              5.152158737182616,
              51.18469636040683
            ]
          ])

minx,miny,maxx,maxy = polygon.bounds
#compute EVI
#https://en.wikipedia.org/wiki/Enhanced_vegetation_index
s2_radiometry = session.imagecollection("CGS_SENTINEL2_RADIOMETRY_V102_001") \
                    .filter_temporal("2017-01-01","2017-10-01") #\
                   # .bbox_filter(left=minx,right=maxx,top=maxy,bottom=miny,srs="EPSG:4326")

B02 = s2_radiometry.band('2')
B04 = s2_radiometry.band('4')
B08 = s2_radiometry.band('8')

evi_cube: ImageCollectionClient = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)

def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)

def load_udf(relative_path):
    import json
    with open(get_test_resource(relative_path), 'r+') as f:
        return f.read()

smoothing_udf = load_udf('udf/smooth_savitzky_golay.py')
#S2 radiometry at VITO already has a default mask otherwise we need a masking function
smoothed_evi = evi_cube.apply_dimension(smoothing_udf,runtime='Python')
timeseries_smooth = smoothed_evi.polygonal_mean_timeseries(polygon)
timeseries_raw_dc = evi_cube.polygonal_mean_timeseries(polygon)

timeseries_raw = pd.Series(timeseries_raw_dc.execute(),name="evi_raw")
timeseries_raw.head(15)


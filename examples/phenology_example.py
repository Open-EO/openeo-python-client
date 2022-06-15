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
session = openeo.connect("openeo.vito.be").authenticate_oidc()
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
    .filter_bbox(west=652000,east=672000,north=5161000,south=5181000,crs=32632) \
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
s2_radiometry = session.load_collection("TERRASCOPE_S2_TOC_V2") \
                    .filter_temporal("2017-01-01","2017-10-01") #\
                   # .filter_bbox(west=minx,east=maxx,north=maxy,south=miny,crs=4326)

B02 = s2_radiometry.band('B04')
B04 = s2_radiometry.band('B04')
B08 = s2_radiometry.band('B08')

evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)

def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)

# TODO #309 #312 Update UDF usage example
smoothing_udf = Path('udf/smooth_savitzky_golay.py').read_text()
#S2 radiometry at VITO already has a default mask otherwise we need a masking function
smoothed_evi = evi_cube.apply_dimension(smoothing_udf,runtime='Python')
timeseries_smooth = smoothed_evi.polygonal_mean_timeseries(polygon)
timeseries_raw_dc = evi_cube.polygonal_mean_timeseries(polygon)

timeseries_raw = pd.Series(timeseries_raw_dc.execute(),name="evi_raw")
print(timeseries_raw.head(15))


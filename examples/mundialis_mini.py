#!/usr/bin/env python3

import json
import openeo
# TODO remove next line, create a "cube" through connection.load_collection()
from openeo.rest.imagecollectionclient import ImageCollectionClient

backend_url = 'https://openeo.mundialis.de'

# definitions
collection_id = 'utm32n.openeo_bolzano.strds.openeo_bolzano_S2'
minx = 10.44662475585937
maxx = 10.62927246093749
maxy = 46.84516443029275
miny = 46.72574176193996
spatial_extent = {'west':minx,'east':maxx,'north':maxy,'south':miny}
temporal_extent=["2018-05-01T00:00:00.000Z","2018-10-01T00:00:00.000Z"]

# To find the band names in GRASS GIS: `g.bands pattern="S2"`
spectral_extent = ["S2_8", "S2_4", "S2_2"]

# connect to mundialis backend
session = openeo.connect(backend_url).authenticate_basic()

# TODO change to s2_radiometry = session.load_collection( ...)
s2_radiometry = ImageCollectionClient.load_collection(
                    session=session,
                    collection_id=collection_id,
                    temporal_extent=temporal_extent,
                    spatial_extent=spatial_extent
                    )            
# g.bands pattern="S2"
B02 = s2_radiometry.band("S2_2")
B04 = s2_radiometry.band("S2_4")
B08 = s2_radiometry.band("S2_8")

evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 10000)

min_evi = evi_cube.min_time()

output = min_evi.download_results("min-evi_mundialis.tiff",format="GTiff")

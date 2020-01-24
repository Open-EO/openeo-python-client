#!/usr/bin/env python3

import json
import openeo
from openeo.rest.imagecollectionclient import ImageCollectionClient

backend_url = 'https://openeo.mundialis.de'
auth_id = "openeo"
auth_pwd = "FIXME"

# definitions
collection_id = 'utm32n.openeo_bolzano.strds.openeo_bolzano_S2'
minx = 10.44662475585937
maxx = 10.62927246093749
maxy = 46.84516443029275
miny = 46.72574176193996
epsg = "EPSG:4326"
spatial_extent = {'west':minx,'east':maxx,'north':maxy,'south':miny,'crs':epsg}
temporal_extent=["2018-05-01T00:00:00.000Z","2018-10-01T00:00:00.000Z"]
spectral_extent = ["B08", "B04", "B02"]

# connect to mundialis backend
session = openeo.connect(backend_url).authenticate_basic(auth_id, password = auth_pwd)

s2_radiometry = ImageCollectionClient.load_collection(
                    session=session,
                    collection_id=collection_id,
                    temporal_extent=temporal_extent,
                    spatial_extent=spatial_extent
                    )            
B02 = s2_radiometry.band("B02")
B04 = s2_radiometry.band("B04")
B08 = s2_radiometry.band("B08")

evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
min_evi = evi_cube.min_time()
output = min_evi.download("min-evi_eodc.tiff",format="GTiff")

#!/usr/bin/env python3

import json
import logging
import openeo
from openeo.rest.imagecollectionclient import ImageCollectionClient

logging.basicConfig(level=logging.INFO)


backend_url = 'https://openeo.mundialis.de'
auth_id = "openeo"
auth_pwd = "FIXME"

collection_id = 'utm32n.openeo_bolzano.strds.openeo_bolzano_S2'

outfile = "/tmp/openeo_mundialis_output.png"

# connect with mundialis openeo/actinia backend
con = openeo.connect(backend_url).authenticate_basic(auth_id, password = auth_pwd)

# Test Connection
print(con.list_processes())
print(con.list_collections())
print(con.describe_collection("utm32n.openeo_bolzano.strds.openeo_bolzano_S2"))
print(con.describe_collection("nc_spm_08.modis_lst.strds.MOD11B3"))


# Test Capabilities
cap = con.capabilities()

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# Test Processes
# North Carolina MODIS LST example
datacube = con.imagecollection("nc_spm_08.modis_lst.strds.MOD11B3.A2016306.h11v05.single_LST_Day_6km")
datacube = datacube.filter_bbox(west=-80.5, south=32.5, east=-70.0, north=38.6, crs="EPSG:4326")
datacube = datacube.filter_temporal(extent=["2016-01-01T00:00:00Z", "2016-03-10T23:59:59Z"])
datacube = datacube.ndvi(nir="S2_4", red="S2_8A")
datacube = datacube.max_time()
print(json.dumps(datacube.graph, indent=2))

# Test Job

# datacube.download("/tmp/testfile.tiff", format="GeoTIFF")

job = con.create_job(datacube.graph)
if job:
    print(job.job_id)
    print(job.start_job())
    print(job.describe_job())
    time.sleep(5)
    job.download_results("/tmp/testfile")
else:
    print("Job ID is None")

#!/usr/bin/env python3

import logging
import openeo
import time

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
print(con.describe_collection("nc_spm_08.modis_lst.strds.LST_Day_monthly"))


# Test Capabilities
cap = con.capabilities()

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# Test Processes
# North Carolina MODIS LST example (TODO: change to con.load_collection() )
datacube = con.imagecollection("utm32n.openeo_bolzano.strds.openeo_bolzano_S2")
datacube = datacube.filter_bbox(west=11.279182, south=46.464349, east=11.406898, north=46.522729, crs="4326")
datacube = datacube.filter_temporal(extent=["2018-05-01T00:00:00Z", "2018-10-10T23:59:59Z"])
datacube = datacube.ndvi()
datacube = datacube.max_time()
print(datacube.to_json())


# Test Job

# datacube.download("/tmp/testfile.tiff", format="GeoTIFF")

job = datacube.send_job()
if job:
    print(job.job_id)
    print(job.start_job())
    print(job.describe_job())
    # TODO: replace with datacube.execute_batch('/tmp/testfile')
    time.sleep(5)
    job.download_results("/tmp/testfile")
else:
    print("Job ID is None")

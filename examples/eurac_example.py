import openeo
import logging
import json
#enable logging in requests library
logging.basicConfig(level=logging.DEBUG)


DRIVER_URL = "http://saocompute.eurac.edu/openEO_0_3_0/openeo"


user = "group1"
password = "test123"


con = openeo.connect(DRIVER_URL, auth_options={"username": user, "password": password})

#Test Connection
print(con.list_processes())
print(con.list_collections())
print(con.describe_collection("S2_L2A_T32TPS_20M"))


# Test Capabilities
cap = con.capabilities()

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

#Example using the 'ImageCollection' API.

datacube = con.imagecollection("S2_L2A_T32TPS_20M")
datacube = datacube.filter_bbox( west=652000, south=5181000, east=672000, north=5161000, crs="EPSG:32632")
datacube = datacube.filter_daterange(extent=["2016-01-01T00:00:00Z", "2016-03-10T23:59:59Z"])
datacube = datacube.ndvi( nir="B04", red="B8A")
datacube = datacube.max_time()
print(json.dumps(datacube.graph,indent=2))

job = con.create_job(datacube.graph)
if job.job_id:
    print(job.job_id)
    print(job.start_job())
    print (job.describe_job())
else:
    print("Job ID is None")

if job.job_id:
    job.download_results("testfile.json")
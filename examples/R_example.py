import logging
import time

import openeo
from openeo.auth.auth_bearer import BearerAuth

logging.basicConfig(level=logging.INFO)


GEE_DRIVER_URL = "https://r-server.openeo.org/"

OUTPUT_FILE = "/tmp/openeo_R_output.png"

user = "test"
password = "test"

# connect with GEE backend
# session = openeo.session("nobody", GEE_DRIVER_URL)

# TODO: update examples
con = openeo.connect(GEE_DRIVER_URL, auth_type=BearerAuth, auth_options={"username": user, "password": password})

# Test Connection
# print(con.list_processes())
# print(con.list_collections())
# print(con.describe_collection("sentinel2_subset"))


# Test Capabilities
# cap = con.capabilities

# print(cap.version())
# print(cap.list_features())
# print(cap.currency())
# print(cap.list_plans())

# Test Processes

processes = con.get_processes()
pg = processes.get_collection(name="sentinel2_subset")
print(pg.to_json())
pg = processes.filter_bbox(pg, west=16.138916, south=-19, east=16.524124, north=-18.9825)
print(pg.to_json())
pg = processes.filter_daterange(pg, extent=["2017-01-01T00:00:00Z", "2017-01-31T23:59:59Z"])
print(pg.to_json())
pg = processes.ndvi(pg, nir="B4", red="B8A")
print(pg.to_json())
pg = processes.min_time(pg)
print(pg.to_json())

# Test Job

job = pg.create_job()
print(job.job_id)
print(job.start_job())
print(job.describe_job())
job.download_results("/tmp/testfile")

import openeo
import logging

logging.basicConfig(level=logging.INFO)


GEE_DRIVER_URL = "http://openeo.vgt.vito.be/openeo"

OUTPUT_FILE = "/tmp/openeo_vito_output.png"

user = "group1"
password = "test123"

user = "group1"
password = "test123"

#connect with GEE backend
#session = openeo.session("nobody", GEE_DRIVER_URL)


con = openeo.connect(GEE_DRIVER_URL, auth_options={"username": user, "password": password})

#Test Connection
print(con.list_processes())
print(con.list_collections())
print(con.describe_collection("BIOPAR_FAPAR_V1_GLOBAL"))


# Test Capabilities
cap = con.capabilities

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# Test Processes

processes = con.get_processes()
pg = processes.get_collection(name="BIOPAR_FAPAR_V1_GLOBAL")
print(pg.graph)
pg = processes.filter_bbox(pg, west=16.138916, south=48.138600, east=16.524124, north=48.320647, crs="EPSG:4326")
print(pg.graph)
pg = processes.filter_daterange(pg, extent=["2016-01-01T00:00:00Z", "2016-03-10T23:59:59Z"])
print(pg.graph)
pg = processes.ndvi(pg, nir="1", red="2")
print(pg.graph)
pg = processes.max_time(pg)
print(pg.graph)

# Test Job

job = con.create_job(pg.graph)
if job:
    print(job.job_id)
    print(job.start_job())
    print(job.describe_job())
    job.download_results("/tmp/testfile")
else:
    print("Job ID is None")
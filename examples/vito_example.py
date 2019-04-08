import openeo
import logging
import json

logging.basicConfig(level=logging.INFO)


VITO_DRIVER_URL = "http://openeo.vgt.vito.be/openeo/0.4.0"

OUTPUT_FILE = "/tmp/openeo_vito_output.png"


con = openeo.connect(VITO_DRIVER_URL)

#Test Connection
print(con.list_processes())
print(con.list_collections())
print(con.describe_collection("BIOPAR_FAPAR_V1_GLOBAL"))


# Test Capabilities
cap = con.capabilities()

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# Test Processes

datacube = con.imagecollection("BIOPAR_FAPAR_V1_GLOBAL")
datacube = datacube.filter_bbox(west=16.138916, south=48.138600, east=16.524124, north=48.320647, crs="EPSG:4326")
datacube = datacube.filter_daterange(extent=["2016-01-01T00:00:00Z", "2016-03-10T23:59:59Z"])
datacube = datacube.max_time()
print(json.dumps(datacube.graph,indent=2))

# Test Job

datacube.download("/tmp/testfile.tiff")

job = con.create_job(datacube.graph)
if job:
    print(job.job_id)
    print(job.start_job())
    print(job.describe_job())
    job.download_results("/tmp/testfile")
else:
    print("Job ID is None")
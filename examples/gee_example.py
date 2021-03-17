import openeo
from openeo.internal.graph_building import PGNode
import logging

logging.basicConfig(level=logging.INFO)

GEE_DRIVER_URL = "https://earthengine.openeo.org/v1.0"

OUTPUT_FILE = "/tmp/openeo_gee_output.png"

# Connect to backend via basic authentication
con = openeo.connect(GEE_DRIVER_URL)
con.authenticate_basic()

# Get information about the backend
print(con.list_processes())
print(con.list_collections())
print(con.describe_collection("COPERNICUS/S2"))

# Test Capabilities
cap = con.capabilities()

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

datacube = con.load_collection("COPERNICUS/S2",
                               spatial_extent={"west": 16.138916, "south": 48.138600, "east": 16.524124, "north": 48.320647, "crs": "EPSG:4326"},
                               temporal_extent=["2017-01-01T00:00:00Z", "2017-01-31T23:59:59Z"],
                               bands=["B4", "B8"])

# Defining complex reducer
red = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "B4"})
nir = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "B8"})
ndvi = PGNode("normalized_difference", arguments={"x": {"from_node": nir}, "y": {"from_node": red}})

datacube = datacube.reduce_dimension(dimension="bands", reducer=ndvi)

print(datacube.to_json())

datacube = datacube.min_time(dimension="t")

# defining linear scale range for apply process
lin_scale = PGNode("linear_scale_range", arguments={"x": {"from_parameter": "x"}, "inputMin": -1, "inputMax": 1, "outputMax": 255})

datacube = datacube.apply(lin_scale)
datacube = datacube.save_result(format="PNG")
print(datacube.to_json())


# Send Job to backend
job = datacube.send_job()
print(job.describe_job())

# Wait for job to finish and download
res = job.start_and_wait().download_results("/tmp")
print(res)

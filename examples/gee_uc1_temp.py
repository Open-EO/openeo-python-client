import openeo
from openeo.internal.graph_building import PGNode
import logging
import time
import json
logging.basicConfig(level=logging.INFO)

GEE_DRIVER_URL = "https://earthengine.openeo.org/v1.0"

user = "group1"
password = "test123"

# Connect to backend via basic authentication
con = openeo.connect(GEE_DRIVER_URL)
con.authenticate_basic(user, password)

datacube = con.load_collection("COPERNICUS/S1_GRD",
                               spatial_extent={"west": 16.06, "south": 48.06, "east": 16.65, "north": 48.35, "crs": "EPSG:4326"},
                               temporal_extent=["2017-03-01", "2017-06-01"],
                               bands=["VV"])
march = datacube.filter_temporal("2017-03-01", "2017-04-01")
april = datacube.filter_temporal("2017-04-01", "2017-05-01")
may = datacube.filter_temporal("2017-05-01", "2017-06-01")

mean_march = march.mean_time(dimension="t")
mean_april = april.mean_time(dimension="t")
mean_may = may.mean_time(dimension="t")

R_band = mean_march.rename_labels(dimension="bands", target=["R"])
G_band = mean_april.rename_labels(dimension="bands", target=["G"])
B_band = mean_may.rename_labels(dimension="bands", target=["B"])

RG = R_band.merge(G_band)
RGB = RG.merge(B_band)

# defining linear scale range for apply process
lin_scale = PGNode("linear_scale_range", arguments={"x": {"from_parameter": "x"},
                                                    "inputMin": -20, "inputMax": -5, "outputMin": 0, "outputMax": 255})

datacube = RGB.apply(lin_scale)
datacube = datacube.save_result(format="PNG", options={"red": "R", "green": "G", "blue": "B"})
print(json.dumps(datacube.graph, indent=2))

# Send Job to backend
job = datacube.send_job()

print(job.job_id)
print(job.start_job())
print(job.describe_job())
time.sleep(5)
print(job.download_results("/tmp/"))
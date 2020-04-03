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
                               temporal_extent=["2017-03-01", "2017-04-01"],
                               bands=["VV", "VH"])

mean = datacube.mean_time(dimension="t")

vh = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "VH"})
vv = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "VV"})
reducer = PGNode("subtract", arguments={"x": {"from_node": vh}, "y": {"from_node": vv}})

datacube = mean.reduce_dimension(dimension="bands", reducer=reducer)

# TODO maybe add "add_dimension" to the datacube class
datacube = datacube.process(process_id='add_dimension',
                 args={"data": {"from_node": datacube._pg}, "name": "bands", "type": "bands"})

# B

blue = datacube.rename_labels(dimension="bands", target=["B"])

lin_scale = PGNode("linear_scale_range", arguments={"x": {"from_parameter": "x"},
                                                    "inputMin": -5, "inputMax": 0, "outputMin": 0, "outputMax": 255})

blue = blue.apply(lin_scale)

# G

green = mean.filter_bands(["VH"])
green = green.rename_labels(dimension="bands", target=["G"])

lin_scale = PGNode("linear_scale_range", arguments={"x": {"from_parameter": "x"},
                                                    "inputMin": -26, "inputMax": -11, "outputMin": 0, "outputMax": 255})

green = green.apply(lin_scale)

# R

red = mean.filter_bands(["VV"])
red = red.rename_labels(dimension="bands", target=["R"])

lin_scale = PGNode("linear_scale_range", arguments={"x": {"from_parameter": "x"},
                                                    "inputMin": -20, "inputMax": -5, "outputMin": 0, "outputMax": 255})

red = red.apply(lin_scale)

# RGB

datacube = red.merge(green).merge(blue)

datacube = datacube.save_result(format="PNG", options={"red": "R", "green": "G", "blue": "B"})
print(json.dumps(datacube.graph, indent=2))

# Send Job to backend
job = datacube.send_job()

print(job.job_id)
print(job.start_job())
print(job.describe_job())
time.sleep(30)
print(job.download_results("/tmp/"))
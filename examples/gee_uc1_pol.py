import openeo
from openeo.internal.graph_building import PGNode

# Connect to backend via basic authentication
con = openeo.connect("https://earthengine.openeo.org/v1.0")
con.authenticate_basic()

datacube = con.load_collection("COPERNICUS/S1_GRD",
                               spatial_extent={"west": 16.06, "south": 48.1, "east": 16.65, "north": 48.31},
                               temporal_extent=["2017-03-01", "2017-04-01"],
                               bands=["VV", "VH"])

mean = datacube.mean_time()

vh = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "VH"})
vv = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "VV"})
reducer = PGNode("subtract", arguments={"x": {"from_node": vh}, "y": {"from_node": vv}})

datacube = mean.reduce_dimension(dimension="bands", reducer=reducer)

# B

blue = datacube.add_dimension(name="bands", label="B", type="bands")

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
print(datacube.to_json())

# Send Job to backend
job = datacube.send_job()

res = job.start_and_wait().download_results()
for key, val in res.items():
    print(key)
zrdz = list(res.keys())[0].name
print("test")
import openeo
import logging
import json
import openeo.internal.processes as pr
# Enable logging in requests library
logging.basicConfig(level=logging.DEBUG)


DRIVER_URL = "https://openeo.eurac.edu"


user = "guest"
password = "guest_123"


con = openeo.connect(DRIVER_URL)
con.authenticate_basic(user, password)
print(con.describe_account())

# get some information about available functionality
cap = con.capabilities()
print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# load a specific dataset
datacube = con.load_collection(session=con, collection_id="S2_L2A_T32TPS_20M",
                               bands=['AOT', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07',
                                      'B8A', 'B11', 'B12', 'SCL', 'VIS', 'WVP', 'CLD', 'SNW'])
# perform spatial subsetting (e.g around the city of Bolzano)
datacube = pr.filter_bbox(cube=datacube, extent={"west": 11.279182434082033, "south": 46.464349400461145,
                                                 "east": 11.406898498535158, "north": 46.522729291844286,
                                                 "crs": "EPSG:32632"})

# perform temporal subsetting (e.g. for the month of august in 2017)
temp = pr.filter_temporal(datacube, extent=["2017-08-01T00:00:00Z", "2017-08-31T00:00:00Z"], dimension=None)
# temp = datacube.filter_temporal(extent=["2017-08-01T00:00:00Z", "2017-08-31T00:00:00Z"])
# map features of the dataset to variables (e.g. the red and near infrared band)
red = temp.band('B04')
nir = temp.band("B8A")
# perform operation using feature variables (e.g. calculation of NDVI (normalized difference vegetation index))
datacube = (nir - red) / (nir + red)
# reduce on temporal dimension with max operator
datacube = pr.max_time(datacube)
# datacube = datacube.max_time()
# provide result as geotiff image
datacube = pr.save_result(datacube, format="gtiff", options={})
# datacube = datacube.save_result(format="gtiff")


# have a look at your process graph (not necessary and only for demonstration purposes)
print(json.dumps(datacube.graph, indent=2))

# submit your process graph as new batch job to back-end
job = con.create_job(datacube.graph)

if job.job_id:
    print(job.job_id)
    print(job.start_job())
    print(job.describe_job())
else:
    print("Job ID is None")

if job.job_id:
    job.download_results("Sentinel2STfile.tiff")

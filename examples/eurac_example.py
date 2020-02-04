import openeo
import json

BACKEND_URL = "https://openeo.eurac.edu"

USER = "guest"
PASSWORD = "guest_123"

# Connect to backend via basic authentication
con = openeo.connect(BACKEND_URL)
con.authenticate_basic(USER, PASSWORD)

# Describe account
print(con.describe_account())

# Get the capacities of the backend
cap = con.capabilities()
print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# List available processes
print(con.list_processes())

# List available collections
print(con.list_collections())

# Load a specific dataset
datacube = con.load_collection(collection_id="S2_L2A_T32TPS_20M", bands=['AOT', 'B02', 'B03', 'B04',
                                                                         'B05', 'B06', 'B07', 'B8A',
                                                                         'B11', 'B12', 'SCL', 'VIS',
                                                                         'WVP', 'CLD', 'SNW'])

# Perform spatial subsetting (e.g around the city of Bolzano)
datacube = datacube.filter_bbox(west=11.279182434082033, south=46.464349400461145, east=11.406898498535158,
                                north=46.522729291844286, crs="EPSG:32632")
# Perform temporal subsetting (e.g. for the month of august in 2017)
temp = datacube.filter_temporal(extent=["2017-08-01T00:00:00Z", "2017-08-31T00:00:00Z"])
# Map features of the dataset to variables (e.g. the red and near infrared band)
red = temp.band('B04')
nir = temp.band("B8A")
# Perform operation using feature variables (e.g. calculation of NDVI (normalized difference vegetation index))
datacube = (nir - red) / (nir + red)
# Reduce on temporal dimension with max operator
datacube = datacube.max_time()
# Provide result as geotiff image
datacube = datacube.save_result(format="gtiff")

print(json.dumps(datacube.graph, indent=2))

# Submit your process graph as new batch job to back-end
job = datacube.send_job()

# Launch processing of submitted batch job
if job.job_id:
    print(job.job_id)
    print(job.start_job())
    print(job.describe_job())
else:
    print("Job ID is None")

# Obtain results and save to disk
if job.job_id:
    job.download_results("Sentinel2STfile.tiff")

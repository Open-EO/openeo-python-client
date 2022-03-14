import openeo

BACKEND_URL = "https://openeo.eurac.edu"

# Connect to backend via basic authentication
con = openeo.connect(BACKEND_URL)
con.authenticate_basic()

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
datacube = con.load_collection(collection_id="openEO_S2_32632_10m_L2A_D22", bands=['B02', 'B03', 'B04', 'B08'])
# Perform spatial subsetting (e.g around the city of Bolzano)
datacube = datacube.filter_bbox(west=11.279182434082033, south=46.464349400461145, 
                                east=11.406898498535158, north=46.522729291844286, crs=32632)
# Perform temporal subsetting (e.g. for the month of june in 2018, only this data available in this collection)
temp = datacube.filter_temporal(extent=["2018-06-06T00:00:00Z", "2018-06-22T00:00:00Z"])
# Map features of the dataset to variables (e.g. the red and near infrared band)
red = temp.band('B04')
nir = temp.band("B08")
# Perform operation using feature variables (e.g. calculation of NDVI (normalized difference vegetation index))
datacube = (nir - red) / (nir + red)
# Reduce on temporal dimension with max operator
datacube = datacube.max_time()
# Provide result as geotiff image
datacube = datacube.save_result(format="gtiff")

print(datacube.to_json())

# Submit your process graph as new batch job to back-end
job = datacube.create_job()

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

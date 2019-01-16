import openeo
import logging

#enable logging in requests library
logging.basicConfig(level=logging.DEBUG)


# TODO: Deprecated: release-0.0.2, Update to 0.3.1 version

GEE_DRIVER_URL = "http://saocompute.eurac.edu/openEO_0_3_0/openeo"


user = "group1"
password = "test123"

#connect with GEE backend
#session = openeo.session("nobody", GEE_DRIVER_URL)


con = openeo.connect(GEE_DRIVER_URL, auth_options={"username": user, "password": password})

#Test Connection
print(con.list_processes())
print(con.list_collections())
print(con.describe_collection("S2_L2A_T32TPS_20M"))


# Test Capabilities
cap = con.capabilities

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# Test Processes

processes = con.get_processes()
pg = processes.get_collection(name="S2_L2A_T32TPS_20M")
print(pg.graph)
pg = processes.filter_bbox(pg, west=652000, south=5181000, east=672000, north=5161000, crs="EPSG:32632")
print(pg.graph)
pg = processes.filter_daterange(pg, extent=["2016-01-01T00:00:00Z", "2016-03-10T23:59:59Z"])
print(pg.graph)
pg = processes.ndvi(pg, nir="B04", red="B8A")
print(pg.graph)
pg = processes.max_time(pg)
print(pg.graph)

# Test Job

job = con.create_job(pg.graph)
if job.job_id:
    print(job.job_id)
    print(job.start_job())
    print (job.describe_job())
    job.download_results("/tmp/testfile")
else:
    print("Job ID is None")
#connect with EURAC backend
#session = openeo.session("nobody", "http://saocompute.eurac.edu/openEO_WCPS_Driver/openeo")

#retrieve the list of available collections
#collections = session.imagecollections()
#print(collections)

#create image collection
#s2_fapar = session.image("S2_L2A_T32TPS_20M")

#specify process graph

#download = s2_fapar.bbox_filter(left=652000,right=672000,top=5161000,bottom=5181000,srs="EPSG:32632")

#download = download.date_range_filter("2016-01-01","2016-03-10")

#download = download.ndvi("B04", "B8A")

#download = download.max_time()

# download = s2_fapar \
#     .date_range_filter("2016-01-01","2016-03-10") \
#     .bbox_filter(left=652000,right=672000,top=5161000,bottom=5181000,srs="EPSG:32632") \
#     .max_time()


#    .download("/tmp/openeo-wcps.geotiff",format="netcdf")
#print(download)



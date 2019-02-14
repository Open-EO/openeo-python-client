import openeo
import logging
import time


logging.basicConfig(level=logging.INFO)


GEE_DRIVER_URL = "http://giv-openeo.uni-muenster.de:8080/v0.3"

OUTPUT_FILE = "/tmp/openeo_gee_output.png"

user = "group1"
password = "test123"

#connect with GEE backend
#session = openeo.session("nobody", GEE_DRIVER_URL)


con = openeo.connect(GEE_DRIVER_URL, auth_options={"username": user, "password": password})

#Test Connection
print(con.list_processes())
print(con.list_collections())
print(con.describe_collection("COPERNICUS/S2"))


# Test Capabilities
cap = con.capabilities

print(cap.version())
print(cap.list_features())
print(cap.currency())
print(cap.list_plans())

# Test Processes

processes = con.get_processes()
pg = processes.get_collection(name="COPERNICUS/S2")
print(pg.graph)
pg = processes.filter_bbox(pg, west=16.138916, south=48.138600, east=16.524124, north=48.320647, crs="EPSG:4326")
print(pg.graph)
pg = processes.filter_daterange(pg, extent=["2017-01-01T00:00:00Z", "2017-01-31T23:59:59Z"])
print(pg.graph)
pg = processes.ndvi(pg, nir="B4", red="B8A")
print(pg.graph)
pg = processes.min_time(pg)
print(pg.graph)

# Test Job

job = con.create_job(pg.graph)
print(job.job_id)
print(job.start_job())
print (job.describe_job())
# time.wait(5)
job.download_results("/tmp/testfile")



# PoC JSON:
# {
#     "process_graph":{
#         "process_id":"stretch_colors",
#         "args":{
#             "imagery":{
#                 "process_id":"min_time",
#                 "args":{
#                     "imagery":{
#                         "process_id":"NDVI",
#                         "args":{
#                             "imagery":{
#                                 "process_id":"filter_daterange",
#                                 "args":{
#                                     "imagery":{
#                                         "process_id":"filter_bbox",
#                                         "args":{
#                                             "imagery":{
#                                                 "product_id":"COPERNICUS/S2"
#                                             },
#                                             "left":9.0,
#                                             "right":9.1,
#                                             "top":12.1,
#                                             "bottom":12.0,
#                                             "srs":"EPSG:4326"
#                                         }
#                                     },
#                                     "from":"2017-01-01",
#                                     "to":"2017-01-31"
#                                 }
#                             },
#                             "red":"B4",
#                             "nir":"B8"
#                         }
#                     }
#                 }
#             },
#             "min": -1,
#             "max": 1
#         }
#     },
#     "output":{
#         "format":"png"
#     }
# }

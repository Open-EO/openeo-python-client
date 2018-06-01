import openeo
import logging
import time


logging.basicConfig(level=logging.DEBUG)


GEE_DRIVER_URL = "http://127.0.0.1:8080"

OUTPUT_FILE = "/tmp/openeo_gee_output.png"


#connect with GEE backend
session = openeo.session("nobody", GEE_DRIVER_URL)

#retrieve the list of available collections
coperincus_s2_image = session.image("COPERNICUS/S2")
logging.debug(coperincus_s2_image.graph)

timeseries = coperincus_s2_image.bbox_filter(left=9.0, right=9.1, top=12.1,
                                             bottom=12.0, srs="EPSG:4326")
logging.debug(timeseries.graph)
timeseries = timeseries.date_range_filter("2017-01-01", "2017-01-31")
logging.debug(timeseries.graph)
timeseries = timeseries.ndvi("B4", "B8")
logging.debug(timeseries.graph)
timeseries = timeseries.min_time()
logging.debug(timeseries.graph)
timeseries = timeseries.strech_colors(-1, 1)
logging.debug(timeseries.graph)
client_job = timeseries.send_job(out_format="png")
logging.debug(client_job.job_id)


client_job.download(OUTPUT_FILE)


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

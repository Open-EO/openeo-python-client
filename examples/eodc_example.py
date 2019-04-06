import logging
import openeo
from openeo.rest.job import RESTJob
from openeo.auth.auth_bearer import BearerAuth

logging.basicConfig(level=logging.DEBUG)


EODC_DRIVER_URL = "http://openeo.eodc.eu"

EODC_USER = "user1"
EODC_PWD = "Test123#"

OUTPUT_FILE = "/tmp/openeo_eodc_output.tiff"

# TODO: Deprecated: release-0.0.2, Update to 0.3.1 version

# Connect with EODC backend
session = openeo.connect(EODC_DRIVER_URL,auth_type=BearerAuth, auth_options={"username": EODC_USER, "password": EODC_PWD})


s2a_prd_msil1c = session.image("s2a_prd_msil1c")
logging.debug("{}".format(s2a_prd_msil1c.graph))

timeseries = s2a_prd_msil1c.bbox_filter(left=652000, right=672000, top=5161000,
                                              bottom=5181000, srs="EPSG:32632")
logging.debug("{}".format(timeseries.graph))

timeseries = timeseries.date_range_filter("2017-01-01", "2017-01-08")
logging.debug("{}".format(timeseries.graph))

timeseries = timeseries.ndvi("B04", "B08")
logging.debug("{}".format(timeseries.graph))

composite = timeseries.min_time()
logging.debug("{}".format(composite.graph))

job = timeseries.send_job()
logging.debug("{}".format(job.job_id))

status = job.queue()
logging.debug("{}".format(status))

# minutes = 0
# minutes_steps = 1
#
# while status != "Finished":
#      time.sleep(minutes_steps*60)
#      minutes += 1
#      status = job.status()['status']
#      logging.debug("After {} minutes the status is: {}".format(minutes, status))

job = RESTJob(97, session)
job.download(OUTPUT_FILE)


# JSON:
#
# {
# "process_graph":{
# "process_id":"min_time",
# "args":{
# "imagery":{
# "process_id":"NDVI",
# "args":{
# "imagery":{
# "process_id":"filter_daterange",
# "args":{
# "imagery":{
# "process_id":"filter_bbox",
# "args":{
# "imagery":{
# "product_id":"s2a_prd_msil1c"
# },
# "left":652000,
# "right":672000,
# "top":5161000,
# "bottom":5181000,
# "srs":"EPSG:32632"
# }
# },
# "from":"2017-01-01",
# "to":"2017-01-08"
# }
# },
# "red":"B04",
# "nir":"B08"
# }
# }
# }
# }
# }

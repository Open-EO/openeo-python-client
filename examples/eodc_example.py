  #
  # def setUp(self):
  #       # configuration phase: define username, endpoint, parameters?
  #       self.endpoint = "http://openeo.eodc.eu"
  #       self.uiser_id = ""
  #       self.auth_id = "user1"
  #       self.auth_pwd = "Test123#"
  #
  #       self.data_id= "s2a_prd_msil1c"
  #       self.process_type = "NDVI"
  #       self.process_date = "filter_daterange"
  #       self.output_file = "/home/berni/test.gtiff"


    # def test_usecase1_eodc(self):
        # m.get("http://localhost:8000/api/auth/login", json={"token": "blabla"})
        # session = openeo.session(self.uiser_id, endpoint=self.endpoint)
        #
        # token = session.auth(self.auth_id, self.auth_pwd)
        #
        # self.assertIsNotNone(token)
        #
        # logging.info("Login token: {}".format(token))
        # s2a_prd_msil1c = session.image("s2a_prd_msil1c")
        # timeseries = s2a_prd_msil1c.date_range_filter("2017-01-01", "2017-01-08").bbox_filter(left=652000, right=672000, top=5161000, bottom=5181000, srs="EPSG:32632")
        #
        # #bandFunction = lambda cells, nodata: (cells[3]-cells[2]/cells[3]+cells[2])
        # #ndvi_timeseries = timeseries.apply_pixel([], bandFunction)
        #
        # composite = timeseries.min_time()
        #
        # print(str(composite.graph))
        #
        #
        # job_id = session.create_job({"process_graph": composite.graph})
        #
        # self.assertIsNotNone(job_id)
        #
        # status = session.queue_job(job_id)
        #
        # self.assertEqual(status, 200)
        #
        # session.download_job(86, "/home/bgoesswe/Documents/openeo-ndvi-composite.geotiff","geotiff")


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

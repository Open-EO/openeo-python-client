import openeo
import logging
import time
import json
import openeo.internal.processes as pr
#from openeo.auth.auth_bearer import BearerAuth

logging.basicConfig(level=logging.INFO)


GEE_DRIVER_URL = "https://earthengine.openeo.org/v0.4"

OUTPUT_FILE = "/tmp/openeo_gee_output.png"

user = "group1"
password = "test123"

#connect with GEE backend
#session = openeo.session("nobody", GEE_DRIVER_URL)

#TODO update example
con = openeo.connect(GEE_DRIVER_URL)
con.authenticate_basic(username=user, password=password)

#Test Connection
#print(con.list_processes())
#print(con.list_collections())
#print(con.describe_collection("COPERNICUS/S2"))


# Test Capabilities
#cap = con.capabilities()

#print(cap.version())
#print(cap.list_features())
#print(cap.currency())
#print(cap.list_plans())

# Test Processes
#datacube = con.load_gen_collection("COPERNICUS/S2")

solution = {
  "load_collection_QHERQ1446J": {
    "process_id": "load_collection",
    "arguments": {
      "id": "COPERNICUS/S2",
      "spatial_extent": {
        "west": -2.7634,
        "south": 43.0408,
        "east": -1.121,
        "north": 43.8385
      },
      "temporal_extent": [
        "2018-04-30",
        "2018-06-26"
      ],
      "bands": [
        "B4",
        "B8"
      ]
    }
  },
  "filter_bands_OKKNR0337R": {
    "process_id": "filter_bands",
    "arguments": {
      "data": {
        "from_node": "load_collection_QHERQ1446J"
      },
      "bands": [
        "B4"
      ]
    }
  },
  "normalized_difference_MIUYD7636T": {
    "process_id": "normalized_difference",
    "arguments": {
      "band1": {
        "from_node": "filter_bands_NDKKL2860V"
      },
      "band2": {
        "from_node": "filter_bands_OKKNR0337R"
      }
    }
  },
  "reduce_EWHEM0849B": {
    "process_id": "reduce",
    "arguments": {
      "data": {
        "from_node": "normalized_difference_MIUYD7636T"
      },
      "reducer": {
        "callback": {
          "min_XLVEQ4794S": {
            "process_id": "min",
            "arguments": {
              "data": {
                "from_argument": "data"
              }
            },
            "result": True
          }
        }
      },
      "dimension": "temporal"
    }
  },
  "apply_KJPGX0184G": {
    "process_id": "apply",
    "arguments": {
      "data": {
        "from_node": "reduce_EWHEM0849B"
      },
      "process": {
        "callback": {
          "linear_scale_range_FSZDJ8749S": {
            "process_id": "linear_scale_range",
            "arguments": {
              "x": {
                "from_argument": "x"
              },
              "inputMin": -1,
              "inputMax": 1,
              "outputMin": 0,
              "outputMax": 255
            },
            "result": True
          }
        }
      }
    }
  },
  "save_result_FXLSK2896A": {
    "process_id": "save_result",
    "arguments": {
      "data": {
        "from_node": "apply_KJPGX0184G"
      },
      "format": "png"
    },
    "result": True
  },
  "filter_bands_NDKKL2860V": {
    "process_id": "filter_bands",
    "arguments": {
      "data": {
        "from_node": "load_collection_QHERQ1446J"
      },
      "bands": [
        "B8"
      ]
    }
  }
}



datacube = con.imagecollection("COPERNICUS/S2")
datacube = datacube.filter_bbox(west=16.138916, south=48.138600, east=16.524124, north=48.320647, crs="EPSG:4326")
datacube_filter = datacube.filter_daterange(extent=["2017-01-01T00:00:00Z", "2017-01-31T23:59:59Z"])
#datacube = datacube.ndvi()#nir="B4", red="B8A")
#red = con.imagecollection("COPERNICUS/S2")
#red = red.filter_bands(["B4"])

#red = datacube.band('B4')
#nir = datacube.band('B8A')
#pr.reduce(datacube, )
#datacube = pr.filter_bands(datacube, ["B4"])
#datacube.graph["reducered"] = pr.reduce(red.graph, "")red.graph
#datacube.graph["reducenir"] = nir.graph
#print(json.dumps(datacube.graph, indent=2))
red_datacube = pr.filter_bands(datacube_filter, ["B4"], from_node=datacube_filter.node_id)
nir_datacube = pr.filter_bands(red_datacube, ["B8A"], from_node=datacube_filter.node_id)
datacube = pr.normalized_difference(nir_datacube, {"from_node": red_datacube.node_id}, {"from_node": nir_datacube.node_id}, "ndvi1")
#datacube = (nir - red) / (nir + red)
datacube = datacube.min_time()
datacube = datacube.save_result(format="PNG")


# Test Job
datacube.graph["saveresult1"]["result"] = True
print(json.dumps(datacube.graph, indent=2))

job = con.create_job(datacube.graph)
if job.job_id:
    print(job.job_id)
    print(job.start_job())
    print (job.describe_job())
else:
    print("Job ID is None!")

time.sleep(10)

if job.job_id:
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

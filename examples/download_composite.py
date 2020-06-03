import logging

import openeo

#enable logging in requests library
logging.basicConfig(level=logging.DEBUG)

#connect with VITO backend
connection = openeo.connect("https://openeo.vito.be")

#retrieve the list of available collections
collections = connection.list_collections()
print(collections)

#create image collection
s2_fapar = connection.load_collection("BIOPAR_FAPAR_V1_GLOBAL",
            spatial_extent={'west':16.138916,'east':16.524124,'south':48.1386,'north':48.320647,'crs':4326},
            temporal_extent=["2016-01-01","2016-03-10"] )

#specify process graph
download = s2_fapar \
    .max_time() \
    .download("/tmp/openeo-composite.geotiff",format="GeoTiff")
print(download)
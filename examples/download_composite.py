import openeo

#connect with VITO backend
connection = openeo.connect("https://openeo.vito.be").authenticate_basic()

#create image collection
s2_fapar = connection.load_collection("TERRASCOPE_S2_FAPAR_V2",
            spatial_extent={'west':16.138916,'east':16.524124,'south':48.1386,'north':48.320647},
            temporal_extent=["2020-05-01","2020-05-20"] )

#specify process graph
download = s2_fapar \
    .max_time() \
    .download("/tmp/openeo-composite.geotiff",format="GTiff")
print(download)
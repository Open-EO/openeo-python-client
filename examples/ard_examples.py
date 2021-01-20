
import openeo

#connect with VITO backend
connection = openeo.connect("https://openeo-dev.vito.be").authenticate_basic("driesj","driesj123")

l1c = connection.load_collection("SENTINEL2_L1C_SENTINELHUB",
            spatial_extent={'west':3.758216409030558,'east':4.087806252,'south':51.291835566,'north':51.3927399,'crs':'EPSG:4326'},
            temporal_extent=["2017-03-07","2017-03-07"],bands=['B04','B03','B02','sunAzimuthAngles','sunZenithAngles','viewAzimuthMean','viewZenithMean'] )

def test_l1c():
    l1c.download("/tmp/openeo-rgb-l1c.geotiff", format="GTiff")

def test_sentinel2_icor():

    #tile 31UES
    3.758216409030558, 51.29183556644949, 4.087806252780558, 51.39273998551565
    #create image collection
    rgb = l1c.atmospheric_correction()

    #specify process graph
    download = rgb \
    .download("/tmp/openeo-rgb-icor.geotiff",format="GTiff")
    print(download)


def test_sentinel2_sen2cor():
    #create image collection
    rgb = connection.load_collection("SENTINEL2_L2A_SENTINELHUB",
            spatial_extent={'west':3.758216409030558,'east':4.087806252,'south':51.291835566,'north':51.3927399,'crs':'EPSG:4326'},
            temporal_extent=["2017-03-07","2017-03-07"],bands=['B04','B03','B02'] )

    #specify process graph
    download = rgb \
    .download("/tmp/openeo-rgb-sen2cor.geotiff",format="GTiff")
    print(download)

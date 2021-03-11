import time

import openeo

#connect with VITO backend
connection = openeo.connect("https://openeo-dev.vito.be").authenticate_basic()

l1c = connection.load_collection("SENTINEL2_L1C_SENTINELHUB",
            spatial_extent={'west':3.758216409030558,'east':4.087806252,'south':51.291835566,'north':51.3927399,'crs':'EPSG:4326'},
            temporal_extent=["2017-03-07","2017-03-07"],bands=['B09','B8A','B11','sunAzimuthAngles','sunZenithAngles','viewAzimuthMean','viewZenithMean'] )

def test_l1c():
    l1c.download("/tmp/openeo-rgb-l1c.geotiff", format="GTiff")

def test_sentinel2_icor():

    #tile 31UES
    3.758216409030558, 51.29183556644949, 4.087806252780558, 51.39273998551565
    #create image collection
    rgb = l1c.atmospheric_correction()

    #specify process graph
    download = rgb.download("/tmp/openeo-rgb-icor.geotiff",format="GTiff")


def test_sentinel2_icor_creo():
    connection = openeo.connect("https://openeo.creo.vito.be").authenticate_basic("driesj","driesj123")

    l1c = connection.load_collection("SENTINEL2_L1C",
                                     spatial_extent={'west': 3.758216409030558, 'east': 4.087806252,
                                                     'south': 51.291835566, 'north': 51.3927399, 'crs': 'EPSG:4326'},
                                     temporal_extent=["2017-03-07","2017-03-07"],
                                     bands=[ 'B02','B09','B8A','B11', 'S2_Level-1C_Tile1_Metadata'])
    l1c = l1c.rename_labels("bands",["B02",'B09','B8A','B11',"SAA","SZA","VAA","VZA"])

    rgb = l1c.atmospheric_correction(method="iCor")
    rgb._pg.arguments['aot'] = 0.2
    rgb._pg.arguments['cwv'] = 0.7
    download = rgb.download("/tmp/openeo-rgb-l2a-icor-creo.geotiff",format="GTiff")
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

from shapely.geometry import Polygon, mapping

polygon = Polygon(shell=[
        [
            5.152158737182616,
            51.18469636040683
        ],
        [
            5.15183687210083,
            51.181979395425095
        ],
        [
            5.152802467346191,
            51.18192559252128
        ],
        [
            5.153381824493408,
            51.184588760878924
        ],
        [
            5.152158737182616,
            51.18469636040683
        ]
    ])


def test_icor_timeseries():
    cloudmask = connection.load_collection("SENTINEL2_L2A_SENTINELHUB",temporal_extent=["2019-02-01", "2019-11-01"],bands=["CLM"])

    l1c = connection.load_collection("SENTINEL2_L1C_SENTINELHUB",
                                     temporal_extent=["2019-02-01", "2019-11-01"],
                                     bands=['B08','B04', 'B8A','B09', 'B11', 'sunAzimuthAngles',
                                            'sunZenithAngles', 'viewAzimuthMean', 'viewZenithMean'])

    start_time = time.time()
    json = l1c.mask(cloudmask).atmospheric_correction().ndvi(nir='B08',red='B04').polygonal_mean_timeseries(polygon).execute()
    elapsed_time = time.time() - start_time
    print("seconds: " + str(elapsed_time))
    from openeo.rest.conversions import timeseries_json_to_pandas
    df = timeseries_json_to_pandas(json)

    l2a = connection.load_collection("SENTINEL2_L2A_SENTINELHUB",
                                     temporal_extent=["2019-02-01", "2019-11-01"], bands=['B08','B04'])
    start_time = time.time()
    l2a_json = l2a.mask(cloudmask).ndvi(nir='B08',red='B04').polygonal_mean_timeseries(polygon).execute()
    elapsed_time = time.time() - start_time
    print("seconds: " + str(elapsed_time))
    df_l2a = timeseries_json_to_pandas(l2a_json)

    import pandas as pd
    df.index = pd.to_datetime(df.index)
    df_l2a.index = pd.to_datetime(df_l2a.index)

    df.name = "iCor"
    df_l2a.name = "Sen2Cor"
    joined = pd.concat([df, df_l2a], axis=1)
    print(joined)
    joined.to_csv('timeseries_icor_masked.csv')

def test_read_plot():
    import pandas as pd
    df = pd.read_csv("/home/driesj/python/openeo-python-client/examples/timeseries_icor_masked.csv")
    df.columns = ["date", "iCor", "Sen2Cor"]
    df.index = pd.to_datetime(df.date)
    df.dropna().plot()
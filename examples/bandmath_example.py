import openeo

connection = openeo.connect("https://openeo.vito.be/openeo/1.0")
connection.authenticate_basic()

sentinel2_data_cube = connection.imagecollection(
    "TERRASCOPE_S2_TOC_V2",
    temporal_extent=["2018-05-06", "2018-05-06"],
    bands=["TOC-B02_10M", "TOC-B04_10M", "TOC-B08_10M"]
)

sentinel2_data_cube = sentinel2_data_cube.filter_bbox(
    west=5.15183687210083,
    east=5.153381824493408,
    south=51.18192559252128,
    north=51.18469636040683,
    crs="EPSG:4326"
)

B02 = sentinel2_data_cube.band('TOC-B02_10M')
B04 = sentinel2_data_cube.band('TOC-B04_10M')
B08 = sentinel2_data_cube.band('TOC-B08_10M')

evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
evi_cube.download("bandmath_example.tiff", format="GTIFF")

openeo.client_version()

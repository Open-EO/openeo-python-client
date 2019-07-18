import openeo
connection = openeo.connect('http://openeo.vgt.vito.be/openeo/0.4.0')
sentinel2_data_cube = connection.imagecollection("CGS_SENTINEL2_RADIOMETRY_V102_001")

sentinel2_data_cube = sentinel2_data_cube.filter_daterange(extent=["2016-01-01","2016-03-10"]) \
                                         .filter_bbox(west=5.15183687210083,east=5.153381824493408,south=51.18192559252128,north=51.18469636040683,crs="EPSG:4326")

B02 = sentinel2_data_cube.band('2')
B04 = sentinel2_data_cube.band('4')
B08 = sentinel2_data_cube.band('8')

evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
evi_cube.download("out.geotiff",format="GeoTIFF")

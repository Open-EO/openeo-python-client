import os
from pathlib import Path

import openeo
from skimage.morphology import selem

connection = openeo.connect("https://openeo-dev.vito.be")
bbox = {"west": 4.996033, "south": 51.258922, "east": 5.091603, "north": 51.282696, "crs": "EPSG:4326"}
connection.authenticate_basic()


sentinel2_data_cube = connection.imagecollection(
    "TERRASCOPE_S2_TOC_V2",
    bands=[ "TOC-B04_10M", "TOC-B08_10M"]
)
sentinel2_data_cube = sentinel2_data_cube.filter_bbox(**bbox)

ndvi = sentinel2_data_cube.ndvi()


scl = connection.imagecollection(
    "TERRASCOPE_S2_TOC_V2",
    bands=["SCENECLASSIFICATION_20M"]
).filter_bbox(**bbox)

classification = scl.band("SCENECLASSIFICATION_20M")


#in openEO, 1 means mask (remove pixel) 0 means keep pixel

SCL_MASK_VALUES = [0, 1, 3, 8, 9, 10, 11]
#keep useful pixels, so set to 1 (remove) if smaller than threshold

#first erode, then dilate: removes noise, in this case small areas marked as cloud?
scl_mask = ~ ((classification == 0) | (classification == 1) | (classification == 3) | (classification == 8) | (classification == 9) | (classification == 10) | (classification == 11))
#scl_mask = ((classification == 3) | (classification == 8) | (classification == 9) )
#this is erosion: clouds (1's) get smaller
scl_mask = scl_mask.apply_kernel(selem.disk(13))
#output of 2D convolution is non-binary, make it binary again, AND invert
scl_mask = scl_mask < 0.01
#now do dilation
scl_mask = scl_mask.apply_kernel(selem.disk(13))
scl_mask = scl_mask.add_dimension(name="bands",label="scl",type="bands")

def test_mask():
    scl_mask.filter_temporal("2018-05-06","2018-05-06").download("mask.tiff",format="GTIFF")

masked = ndvi.mask(scl_mask)

#I'm using pytest methods here: PyCharm allows these methods to be started individually, which is more convenient
# otherwise my script would be doing multiple downloads on each test, or I would have to comment out these lines

#takes a few seconds to download
def test_only_ndvi():
    masked.filter_temporal("2018-05-06","2018-05-06").download("ndvi_composite.tiff", format="GTiff")

#client side logic is needed to construct input intervals and output labels
#this gives you the flexibility to do all kinds of compositing
start_date = "2018-04-21"
end_date = "2018-05-31"
#TODO: flat list for intervals is maybe a bit weird?
#EP-3616 support median reducer
composited_ndvi = masked.aggregate_temporal(intervals=[start_date,"2018-05-21","2018-05-01",end_date],labels=["2018-05-01","2018-05-11"],reducer=lambda d:d.mean(),dimension='t')

def test_masked_netcdf():
    composited_ndvi.filter_temporal(start_date,end_date).download("masked.nc",format="NetCDF")

def test_composite_netcdf():
    composited_ndvi.filter_temporal(start_date,end_date).download("composite.nc",format="NetCDF")

def test_composite_geotiff():
    composited_ndvi.filter_temporal(start_date,end_date).download("composite.tiff",format="GTIFF")


def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)

def load_udf(relative_path):
    with open(get_test_resource(relative_path), 'r+') as f:
        return f.read()

compositing_udf = load_udf('udf/median_composite.py')

def test_composite_by_udf():
    masked.apply_dimension(code=compositing_udf,runtime="Python",dimension='t').filter_temporal(start_date,end_date).download("composite_udf.nc",format="NetCDF")

from openeo.rest.datacube import DataCube

def test_debug_udf():
    """
    Shows how to run your UDF locally for testing. This method uses the same code as the backend, and can be used to check validity of your UDF.
    https://open-eo.github.io/openeo-python-client/udf.html#example-downloading-a-datacube-and-executing-an-udf-locally
    depends on composite.nc file created in earlier function!
    """

    DataCube.execute_local_udf(compositing_udf, 'masked.nc', fmt='netcdf')

from examples.udf.median_composite import apply_datacube

def test_debug_udf_direct_invoke():
    """
    Shows how to run your UDF locally for testing, by invoking the function directly, breakpoints work.
    https://open-eo.github.io/openeo-python-client/udf.html#example-downloading-a-datacube-and-executing-an-udf-locally
    depends on composite.nc file created in earlier function!
    """
    from openeo.udf.xarraydatacube import XarrayDataCube
    udf_cube = XarrayDataCube.from_file('masked.nc', fmt='netcdf')

    apply_datacube(udf_cube,context={})

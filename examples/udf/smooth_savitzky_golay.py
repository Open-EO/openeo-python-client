# -*- coding: utf-8 -*-
# Uncomment the import only for coding support
#from openeo_udf.api.base import SpatialExtent, RasterCollectionTile, FeatureCollectionTile, UdfData

__license__ = "Apache License, Version 2.0"


def rct_savitzky_golay(udf_data):
    from scipy.signal import savgol_filter

    # Iterate over each tile
    for tile in udf_data.raster_collection_tiles:
        timeseries_array = tile.data
        smoothed_array = savgol_filter(timeseries_array, 5, 2)
        #print(smoothed_array)
        tile.set_data(smoothed_array)


# This function call is the entry point for the UDF.
# The caller will provide all required data in the **data** object.
rct_savitzky_golay(data)

# -*- coding: utf-8 -*-
# Uncomment the import only for coding support
#from openeo_udf.api.base import SpatialExtent, RasterCollectionTile, FeatureCollectionTile, UdfData

__license__ = "Apache License, Version 2.0"


def rct_savitzky_golay(udf_data):
    from scipy.signal import savgol_filter
    import pandas as pd
    # Iterate over each tile
    for tile in udf_data.raster_collection_tiles:
        timeseries_array = tile.data
        #TODO: savitzky golay implementation assumes regularly spaced samples!

        #first we ensure that there are no nodata values in our input, as this will cause everything to become nodata.
        array_2d = timeseries_array.reshape((timeseries_array.shape[0], timeseries_array.shape[1] * timeseries_array.shape[2]))

        df = pd.DataFrame(array_2d)
        #df.fillna(method='ffill', axis=0, inplace=True)
        df.interpolate(inplace=True,axis=0)
        filled=df.as_matrix().reshape(timeseries_array.shape)

        #now apply savitzky golay on filled data
        smoothed_array = savgol_filter(filled, 5, 1,axis=0)
        #print(smoothed_array)
        tile.set_data(smoothed_array)


# This function call is the entry point for the UDF.
# The caller will provide all required data in the **data** object.
rct_savitzky_golay(data)

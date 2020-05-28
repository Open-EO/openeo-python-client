# -*- coding: utf-8 -*-
# Uncomment the import only for coding support
from typing import Dict

import xarray
from openeo_udf.api.datacube import DataCube


def apply_hypercube(cube: DataCube, context: Dict) -> DataCube:
    """
    Applies a savitzky-golay smoothing to a timeseries datacube.
    This UDF preserves dimensionality, and assumes a datacube with a temporal dimension 't' as input.
    """
    from scipy.signal import savgol_filter

    array: xarray.DataArray = cube.get_array()
    filled = array.interpolate_na(dim='t')
    smoothed_array = savgol_filter(filled.values, 5, 2, axis=0)
    return DataCube(xarray.DataArray(smoothed_array,dims=array.dims,coords=array.coords))


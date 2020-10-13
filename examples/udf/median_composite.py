# -*- coding: utf-8 -*-
from typing import Dict

import xarray
from openeo_udf.api.datacube import DataCube


def apply_datacube(cube: DataCube, context: Dict) -> DataCube:
    """
    Applies a rolling window median composite to a timeseries datacube.
    This UDF preserves dimensionality, and assumes a datacube with a temporal dimension 't' as input.
    """

    array: xarray.DataArray = cube.get_array()

    import pandas as pd
    import numpy as np

    #this method computes dekad's, can be used to resample data to desired frequency

    time_dimension_index = array.get_index('t')

    d = time_dimension_index.day - np.clip((time_dimension_index.day - 1) // 10, 0, 2) * 10 - 1
    date = time_dimension_index.values - np.array(d, dtype="timedelta64[D]")

    #replace each value with 30-day window median
    #first median rolling window to fill gaps on all dates
    composited = array.rolling(t=30,min_periods=1, center=True).median().dropna("t")
    #resample rolling window medians to dekads
    ten_daily_composite = composited.groupby_bins("t",date).median()
    return DataCube(ten_daily_composite)
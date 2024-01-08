import xarray
from scipy.signal import savgol_filter

from openeo.udf import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Apply Savitzky-Golay smoothing to a timeseries datacube.
    This UDF preserves dimensionality, and assumes an input
    datacube with a temporal dimension 't' as input.
    """
    array: xarray.DataArray = cube.get_array()
    filled = array.interpolate_na(dim="t")
    smoothed_array = savgol_filter(filled.values, 5, 2, axis=0)
    return XarrayDataCube(array=xarray.DataArray(smoothed_array, dims=array.dims, coords=array.coords))

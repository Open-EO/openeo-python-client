import xarray
from scipy.signal import savgol_filter

from openeo.udf.xarraydatacube import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Applies a savitzky-golay smoothing to a timeseries datacube.
    This UDF preserves dimensionality, and assumes a datacube with a temporal dimension 't' as input.
    """
    array: xarray.DataArray = cube.get_array()
    filled = array.interpolate_na(dim='t')
    smoothed_array = savgol_filter(filled.values, 5, 2, axis=0)
    return XarrayDataCube(xarray.DataArray(smoothed_array, dims=array.dims, coords=array.coords))

"""
This module defines a number of function signatures that can be implemented by UDF's.
Both the name of the function and the argument types are/can be used by the backend to validate if the provided UDF
is compatible with the calling context of the process graph in which it is used.

"""
# Note: this module was initially developed under the ``openeo-udf`` project (https://github.com/Open-EO/openeo-udf)

from pandas import Series

from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube


def apply_timeseries(series: Series, context: dict) -> Series:
    """
    Process a timeseries of values, without changing the time instants.

    This can for instance be used for smoothing or gap-filling.

    :param series: A Pandas Series object with a date-time index.
    :param context: A dictionary containing user context.
    :return: A Pandas Series object with the same datetime index.
    """
    # TODO: do we need geospatial coordinates for the series?
    return series


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Map a :py:class:`XarrayDataCube` to another :py:class:`XarrayDataCube`.

    Depending on the context in which this function is used, the :py:class:`XarrayDataCube` dimensions
    have to be retained or can be chained.
    For instance, in the context of a reducing operation along a dimension,
    that dimension will have to be reduced to a single value.
    In the context of a 1 to 1 mapping operation, all dimensions have to be retained.

    :param cube: input data cube
    :param context: A dictionary containing user context.
    :return: output data cube
    """
    return cube


def apply_udf_data(data: UdfData):
    """
    Generic UDF function that directly manipulates a :py:class:`UdfData` object

    :param data: :py:class:`UdfData` object to manipulate in-place
    """
    pass

"""
This module defines a number of function signatures that can be implemented by UDF's.
Both the name of the function and the argument types are/can be used by the backend to validate if the provided UDF
is compatible with the calling context of the process graph in which it is used.

"""
# Note: this module was initially developed under the ``openeo-udf`` project (https://github.com/Open-EO/openeo-udf)

import xarray
from pandas import Series

from openeo.metadata import CollectionMetadata
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube

try:
    # Geopandas is an optional dependency, but one of the signatures uses it as type annotation
    import geopandas
except ImportError:
    pass


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


def apply_metadata(metadata: CollectionMetadata, context: dict) -> CollectionMetadata:
    """
    .. warning::
        This signature is not yet fully standardized and subject to change.

    Returns the expected cube metadata, after applying this UDF, based on input metadata.
    The provided metadata represents the whole raster or vector cube. This function does not need to be called for every data chunk.

    When this function is not implemented by the UDF, the backend may still be able to infer correct metadata by running the
    UDF, but this can result in reduced performance or errors.

    This function does not need to be provided when using the UDF in combination with processes that by design have a clear
    effect on cube metadata, such as :py:meth:`~openeo.rest.datacube.DataCube.reduce_dimension()`

    :param metadata: the collection metadata of the input data cube
    :param context: A dictionary containing user context.

    :return: output metadata: the expected metadata of the cube, after applying the udf

    Examples
    --------

    An example for a UDF that is applied on the 'bands' dimension, and returns a new set of bands with different labels.

    >>> def apply_metadata(metadata: CollectionMetadata, context: dict) -> CollectionMetadata:
    ...     return metadata.rename_labels(
    ...         dimension="bands",
    ...         target=["computed_band_1", "computed_band_2"]
    ...     )

    """
    pass


def apply_vectorcube(
    geometries: "geopandas.geodataframe.GeoDataFrame", cube: xarray.DataArray, context: dict
) -> ("geopandas.geodataframe.GeoDataFrame", xarray.DataArray):
    """
    Map a vector cube to another vector cube.

    :param geometries: input geometries as a geopandas.GeoDataFrame. This contains the actual shapely geometries and optional properties.
    :param cube: a data cube with dimensions (geometries, time, bands) where time and bands are optional.
        The coordinates for the geometry dimension are integers and match the index of the geometries in the geometries parameter.
    :param context: A dictionary containing user context.
    :return: output geometries, output data cube
    """
    pass

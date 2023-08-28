"""
Helpers for data conversions between Python ecosystem data types and openEO data structures.
"""

from __future__ import annotations

import typing

import numpy as np
import pandas

from openeo.internal.warnings import deprecated

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    import xarray

    from openeo.udf import XarrayDataCube


class InvalidTimeSeriesException(ValueError):
    pass


def timeseries_json_to_pandas(timeseries: dict, index: str = "date", auto_collapse=True) -> pandas.DataFrame:
    """
    Convert a timeseries JSON object as returned by the `aggregate_spatial` process to a pandas DataFrame object

    This timeseries data has three dimensions in general: date, polygon index and band index.
    One of these will be used as index of the resulting dataframe (as specified by the `index` argument),
    and the other two will be used as multilevel columns.
    When there is just a single polygon or band in play, the dataframe will be simplified
    by removing the corresponding dimension if `auto_collapse` is enabled (on by default).

    :param timeseries: dictionary as returned by `aggregate_spatial`
    :param index: which dimension should be used for the DataFrame index: 'date' or 'polygon'
    :param auto_collapse: whether single band or single polygon cases should be simplified automatically

    :return: pandas DataFrame or Series
    """
    # The input timeseries dictionary is assumed to have this structure:
    #       {dict mapping date -> [list with one item per polygon: [list with one float/None per band or empty list]]}
    # TODO is this format of `aggregate_spatial` standardized across backends? Or can we detect the structure?
    # TODO: option to pass a path to a JSON file as input?

    # Some quick checks
    if len(timeseries) == 0:
        raise InvalidTimeSeriesException("Empty data set")
    polygon_counts = set(len(polygon_data) for polygon_data in timeseries.values())
    if polygon_counts == {0}:
        raise InvalidTimeSeriesException("No polygon data for each date")
    elif 0 in polygon_counts:
        # TODO: still support this use case?
        raise InvalidTimeSeriesException("No polygon data for some dates ({p})".format(p=polygon_counts))
    elif len(polygon_counts) > 1:
        raise InvalidTimeSeriesException("Inconsistent polygon counts: {p}".format(p=polygon_counts))
    # Count the number of bands in the timeseries, so we can provide a fallback array for missing data
    band_counts = set(len(band_data) for polygon_data in timeseries.values() for band_data in polygon_data)
    if band_counts == {0}:
        raise InvalidTimeSeriesException("Zero bands everywhere")
    band_counts.discard(0)
    if len(band_counts) != 1:
        raise InvalidTimeSeriesException("Inconsistent band counts: {b}".format(b=band_counts))
    band_count = band_counts.pop()
    band_data_fallback = [np.nan] * band_count
    # Load the timeseries data in a pandas Series with multi-index ["date", "polygon", "band"]
    s = pandas.DataFrame.from_records(
        (
            (date, polygon_index, band_index, value)
            for (date, polygon_data) in timeseries.items()
            for polygon_index, band_data in enumerate(polygon_data)
            for band_index, value in enumerate(band_data or band_data_fallback)
        ),
        columns=["date", "polygon", "band", "value"],
        index=["date", "polygon", "band"]
    )["value"].rename(None)
    # TODO convert date to real date index?

    if auto_collapse:
        if s.index.levshape[2] == 1:
            # Single band case
            s.index = s.index.droplevel("band")
        if s.index.levshape[1] == 1:
            # Single polygon case
            s.index = s.index.droplevel("polygon")

    # Reshape as desired
    if index == "date":
        if len(s.index.names) > 1:
            return s.unstack("date").T
        else:
            return s
    elif index == "polygon":
        return s.unstack("polygon").T
    else:
        raise ValueError(index)


@deprecated("Use :py:meth:`XarrayDataCube.from_file` instead.", version="0.7.0")
def datacube_from_file(filename, fmt="netcdf") -> XarrayDataCube:
    from openeo.udf.xarraydatacube import XarrayDataCube
    return XarrayDataCube.from_file(path=filename, fmt=fmt)


@deprecated("Use :py:meth:`XarrayDataCube.save_to_file` instead.", version="0.7.0")
def datacube_to_file(datacube: XarrayDataCube, filename, fmt="netcdf"):
    return datacube.save_to_file(path=filename, fmt=fmt)


@deprecated("Use :py:meth:`XarrayIO.to_json_file` instead", version="0.7.0")
def _save_DataArray_to_JSON(filename, array: xarray.DataArray):
    from openeo.udf.xarraydatacube import XarrayIO
    return XarrayIO.to_json_file(array=array, path=filename)


@deprecated("Use :py:meth:`XarrayIO.to_netcdf_file` instead", version="0.7.0")
def _save_DataArray_to_NetCDF(filename, array: xarray.DataArray):
    from openeo.udf.xarraydatacube import XarrayIO
    return XarrayIO.to_netcdf_file(array=array, path=filename)


@deprecated("Use :py:meth:`XarrayDataCube.plot` instead.", version="0.7.0")
def datacube_plot(datacube: XarrayDataCube, *args, **kwargs):
    datacube.plot(*args, **kwargs)

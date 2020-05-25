"""
Helpers for data conversions between Python ecosystem data types and openEO data structures.
"""

import numpy as np
import pandas


class InvalidTimeSeriesException(ValueError):
    pass


def timeseries_json_to_pandas(timeseries: dict, index: str = "date", auto_collapse=True) -> pandas.DataFrame:
    """
    Convert a timeseries JSON object as returned by the `aggregate_polygon` process to a pandas DataFrame object

    This timeseries data has three dimensions in general: date, polygon index and band index.
    One of these will be used as index of the resulting dataframe (as specified by the `index` argument),
    and the other two will be used as multilevel columns.
    When there is just a single polygon or band in play, the dataframe will be simplified
    by removing the corresponding dimension if `auto_collapse` is enabled (on by default).

    :param timeseries: dictionary as returned by `aggregate_polygon` (TODO: is this standardized?)
    :param index: which dimension should be used for the DataFrame index: 'date' or 'polygon'
    :param auto_collapse: whether single band or single polygon cases should be simplified automatically

    :return: pandas DataFrame or Series
    """
    # The input timeseries dictionary is assumed to have this structure:
    #       {dict mapping date -> [list with one item per polygon: [list with one float/None per band or empty list]]}

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

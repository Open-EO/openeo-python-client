"""
Helpers for data conversions between Python ecosystem data types and openEO data structures.
"""

import numpy as np
import pandas


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
    #       {dict mapping date -> [list with one item per polygon: [list with one float/None per band or empty list]}
    # Load the timeseries data in a DataFrame with "band" index and ("date", "polygon") multi-level columns
    df = pandas.DataFrame.from_dict({
        (ts, polygon_index): band_data
        for (ts, values) in timeseries.items()
        for polygon_index, band_data in enumerate(values)
        if band_data
    }).fillna(value=np.nan)
    df.index.name = "band"
    df.columns.names = ["date", "polygon"]
    # TODO convert date to real date index?
    # Reshape to multi-indexed series (index: "date", "polygon", "band) for easier manipulation below.
    s = df.unstack("band")

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

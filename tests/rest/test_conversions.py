import numpy as np
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal, assert_series_equal

from openeo.rest.conversions import timeseries_json_to_pandas

DATE1 = "2019-01-11T11:11:11Z"
DATE2 = "2019-02-22T22:22:22Z"
DATE3 = "2019-03-03T03:03:03Z"
DATE4 = "2019-04-04T04:04:04Z"


def test_timeseries_json_to_pandas_basic():
    # 2 dates, 2 polygons, 3 bands
    timeseries = {
        DATE1: [[1, 2, 3], [4, 5, 6]],
        DATE2: [[7, 8, 9], [10, 11, 12]],
    }
    df = timeseries_json_to_pandas(timeseries)
    expected = pd.DataFrame(data=[
        [1, 2, 3, 4, 5, 6],
        [7, 8, 9, 10, 11, 12]
    ],
        index=pd.Index([DATE1, DATE2], name="date"),
        columns=pd.MultiIndex.from_tuples(
            [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)],
            names=("polygon", "band")
        )
    )
    assert_frame_equal(df, expected)


def test_timeseries_json_to_pandas_index_polygon():
    # 2 dates, 2 polygons, 3 bands
    timeseries = {
        DATE1: [[1, 2, 3], [4, 5, 6]],
        DATE2: [[7, 8, 9], [10, 11, 12]],
    }
    df = timeseries_json_to_pandas(timeseries, index="polygon")
    expected = pd.DataFrame(data=[
        [1, 2, 3, 7, 8, 9],
        [4, 5, 6, 10, 11, 12]
    ],
        index=pd.Index([0, 1], name="polygon"),
        columns=pd.MultiIndex.from_tuples(
            [(DATE1, 0), (DATE1, 1), (DATE1, 2),
             (DATE2, 0), (DATE2, 1), (DATE2, 2)],
            names=("date", "band")
        )
    )
    assert_frame_equal(df, expected)


@pytest.mark.parametrize(["timeseries", "index", "with_auto_collapse", "without_auto_collapse"], [
    (
            # 2 dates, 2 polygons, 1 band. With date index
            {DATE1: [[1], [4]], DATE2: [[7], [10]]},
            "date",
            pd.DataFrame(
                data=[[1, 4], [7, 10]],
                index=pd.Index([DATE1, DATE2], name="date"),
                columns=pd.Index([0, 1], name="polygon")
            ),
            pd.DataFrame(
                data=[[1, 4], [7, 10]],
                index=pd.Index([DATE1, DATE2], name="date"),
                columns=pd.MultiIndex.from_tuples([(0, 0), (1, 0)], names=("polygon", "band"))
            ),
    ),
    (
            # 2 dates, 2 polygons, 1 band. With polygon index
            {DATE1: [[1], [4]], DATE2: [[7], [10]]},
            "polygon",
            pd.DataFrame(
                data=[[1, 7], [4, 10]],
                index=pd.Index([0, 1], name="polygon"),
                columns=pd.Index([DATE1, DATE2], name="date")
            ),
            pd.DataFrame(
                data=[[1, 7], [4, 10]],
                index=pd.Index([0, 1], name="polygon"),
                columns=pd.MultiIndex.from_tuples([(DATE1, 0), (DATE2, 0)], names=("date", "band"))
            ),
    ),
    (
            # 2 dates, 1 polygon, 3 bands. With date index
            {DATE1: [[1, 2, 3]], DATE2: [[7, 8, 9]]},
            "date",
            pd.DataFrame(
                data=[[1, 2, 3], [7, 8, 9]],
                index=pd.Index([DATE1, DATE2], name="date"),
                columns=pd.Index([0, 1, 2], name="band")
            ),
            pd.DataFrame(
                data=[[1, 2, 3], [7, 8, 9]],
                index=pd.Index([DATE1, DATE2], name="date"),
                columns=pd.MultiIndex.from_tuples([(0, 0), (0, 1), (0, 2)], names=("polygon", "band"))
            ),
    ),
    (
            # 2 dates, 1 polygon, 1 band.
            {DATE1: [[1]], DATE2: [[7]]},
            "date",
            pd.Series(
                data=[1, 7],
                index=pd.Index([DATE1, DATE2], name="date"),
            ),
            pd.DataFrame(
                data=[[1], [7]],
                index=pd.Index([DATE1, DATE2], name="date"),
                columns=pd.MultiIndex.from_tuples([(0, 0)], names=("polygon", "band"))
            ),
    ),
])
def test_timeseries_json_to_pandas_auto_collapse(timeseries, index, with_auto_collapse, without_auto_collapse):
    df = timeseries_json_to_pandas(timeseries, index=index, auto_collapse=True)
    if isinstance(with_auto_collapse, pd.Series):
        assert_series_equal(df, with_auto_collapse)
    else:
        assert_frame_equal(df, with_auto_collapse)
    df = timeseries_json_to_pandas(timeseries, index=index, auto_collapse=False)
    assert_frame_equal(df, without_auto_collapse)


def test_timeseries_json_to_pandas_none_nan_empty_handling():
    timeseries = {
        DATE1: [[1, 2], [3, 4]],
        DATE2: [[5, 6], [None, None]],
        DATE3: [[], []],
        DATE4: [[], [7, 8]],
    }
    df = timeseries_json_to_pandas(timeseries)
    expected = pd.DataFrame(data=[
        [1, 2, 3, 4],
        [5, 6, np.nan, np.nan],
        [np.nan, np.nan, np.nan, np.nan],
        [np.nan, np.nan, 7, 8],
    ],
        dtype=float,
        index=pd.Index([DATE1, DATE2, DATE3, DATE4], name="date"),
        columns=pd.MultiIndex.from_tuples(
            [(0, 0), (0, 1), (1, 0), (1, 1)],
            names=("polygon", "band")
        )
    )
    assert_frame_equal(df, expected)

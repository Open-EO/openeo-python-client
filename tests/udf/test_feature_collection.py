import pytest
from geopandas import GeoDataFrame
from pandas import DatetimeIndex
from shapely.geometry import Point

from openeo.udf import FeatureCollection


def test_feature_collection_basic():
    geometry = [Point(3, 5)]
    data = GeoDataFrame({"a": [1], "b": [2]}, geometry=geometry)
    fc = FeatureCollection(id="test", data=data)
    assert fc.to_dict() == {
        "id": "test",
        "data": {
            "type": "FeatureCollection",
            "features": [
                {
                    "id": "0",
                    "type": "Feature",
                    "geometry": {"coordinates": (3.0, 5.0), "type": "Point"},
                    "properties": {"a": 1, "b": 2},
                    "bbox": (3.0, 5.0, 3.0, 5.0),
                },
            ],
            "bbox": (3.0, 5.0, 3.0, 5.0),
        },
    }


def test_feature_collection_with_times():
    geometry = [Point(1, 2), Point(3, 5), Point(8, 13)]
    data = GeoDataFrame({"a": [11, 22, 33], "b": [55, 88, 13]}, geometry=geometry)
    start_times = ["2021-01-01", "2021-02-01", "2021-03-01"]
    end_times = ["2021-01-10", "2021-02-10", "2021-03-10"]
    fc = FeatureCollection(id="test", data=data, start_times=start_times, end_times=end_times)
    assert fc.to_dict() == {
        "id": "test",
        "data": {
            "type": "FeatureCollection",
            "features": [
                {
                    "id": "0", "type": "Feature", "properties": {"a": 11, "b": 55},
                    "geometry": {"coordinates": (1.0, 2.0), "type": "Point"}, "bbox": (1.0, 2.0, 1.0, 2.0),
                },
                {
                    "id": "1", "type": "Feature", "properties": {"a": 22, "b": 88},
                    "geometry": {"coordinates": (3.0, 5.0), "type": "Point"}, "bbox": (3.0, 5.0, 3.0, 5.0),
                },
                {
                    "id": "2", "type": "Feature", "properties": {"a": 33, "b": 13},
                    "geometry": {"coordinates": (8.0, 13.0), "type": "Point"}, "bbox": (8.0, 13.0, 8.0, 13.0),
                },
            ],
            "bbox": (1.0, 2.0, 8.0, 13.0),
        },
        "start_times": ['2021-01-01T00:00:00', '2021-02-01T00:00:00', '2021-03-01T00:00:00'],
        "end_times": ['2021-01-10T00:00:00', '2021-02-10T00:00:00', '2021-03-10T00:00:00'],
    }


@pytest.mark.parametrize(["start_times", "expected"], [
    (None, None),
    (["2021-01-02"], DatetimeIndex(["2021-01-02"])),
    (DatetimeIndex(["2021-01-02"]), DatetimeIndex(["2021-01-02"])),
])
def test_feature_collection_start_times(start_times, expected):
    data = GeoDataFrame({"a": [1], "b": [2]}, geometry=[Point(3, 5)])
    fc = FeatureCollection(id="test", data=data, start_times=start_times)
    assert fc.start_times == expected


@pytest.mark.parametrize(["end_times", "expected"], [
    (None, None),
    (["2021-01-02"], DatetimeIndex(["2021-01-02"])),
    (DatetimeIndex(["2021-01-02"]), DatetimeIndex(["2021-01-02"])),
])
def test_feature_collection_end_times(end_times, expected):
    data = GeoDataFrame({"a": [1], "b": [2]}, geometry=[Point(3, 5)])
    fc = FeatureCollection(id="test", data=data, end_times=end_times)
    assert fc.end_times == expected


@pytest.mark.parametrize(["dates", "exc"], [
    ([], "Expected size 1 but got 0"),
    (["2021-01-02", "2021-01-05"], "Expected size 1 but got 2")
])
def test_feature_collection_wrong_times(dates, exc):
    data = GeoDataFrame({"a": [1], "b": [2]}, geometry=[Point(3, 5)])
    with pytest.raises(ValueError, match=exc):
        FeatureCollection(id="test", data=data, start_times=dates)
    with pytest.raises(ValueError, match=exc):
        FeatureCollection(id="test", data=data, end_times=dates)


def test_feature_collection_from_dict():
    d = {
        "id": "test",
        "data": {
            "type": "FeatureCollection",
            "features": [
                {
                    "id": 0, "type": "Feature",
                    "geometry": {"coordinates": (3.0, 5.0), "type": "Point"},
                    "properties": {"a": 1, "b": 2},
                },
            ],
        },
        "start_times": ['2021-01-01T00:00:00'],
    }
    fc = FeatureCollection.from_dict(d)
    assert fc.id == "test"
    df = fc.data
    assert set(df.columns) == {"geometry", "a", "b"}
    assert set(df.index) == {0}
    assert df.shape == (1, 3)
    assert df.iloc[0].to_dict() == {"a": 1, "b": 2, "geometry": Point(3, 5)}

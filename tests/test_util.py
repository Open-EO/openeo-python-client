import logging
import os
import pathlib
import re
import unittest.mock as mock
from datetime import datetime, date
from typing import List, Union

import pytest

from openeo.util import first_not_none, get_temporal_extent, TimingLogger, ensure_list, ensure_dir, dict_no_none, \
    deep_get, DeepKeyError, get_user_config_dir, get_user_data_dir, Rfc3339, rfc3339, deep_set, legacy_alias, \
    LazyLoadCache, guess_format, ContextTimer


def test_rfc3339_date():
    assert "2020-03-17" == rfc3339.date("2020-03-17")
    assert "2020-03-17" == rfc3339.date("2020/03/17")
    assert "2020-03-17" == rfc3339.date("2020:03:17")
    assert "2020-03-17" == rfc3339.date("2020_03_17")
    assert "2020-03-17" == rfc3339.date("2020-03-17-12-34-56")
    assert "2020-03-17" == rfc3339.date("2020-03-17-12-34")
    assert "2020-03-17" == rfc3339.date("2020:03:17:12:34:56")
    assert "2020-03-17" == rfc3339.date("2020/03/17/12/34/56")
    assert "2020-03-17" == rfc3339.date("2020/03/17/12/34")
    assert "2020-03-17" == rfc3339.date("2020_03_17_12_34_56")
    assert "2020-03-17" == rfc3339.date("2020-03-17T12:34:56Z")
    assert "2020-03-17" == rfc3339.date(date(2020, 3, 17))
    assert "2020-03-17" == rfc3339.date(datetime(2020, 3, 17, 12, 34, 56))
    assert "2020-03-17" == rfc3339.date((2020, 3, 17))
    assert "2020-03-17" == rfc3339.date([2020, 3, 17])
    assert "2020-03-17" == rfc3339.date(2020, 3, 17)
    assert "2020-03-17" == rfc3339.date((2020, 3, 17, 12, 34, 56))
    assert "2020-03-17" == rfc3339.date([2020, 3, 17, 12, 34, 56])
    assert "2020-03-17" == rfc3339.date(2020, 3, 17, 12, 34, 56)
    assert "2020-03-17" == rfc3339.date(2020, 3, 17, 12, 34)
    assert "2020-03-17" == rfc3339.date(("2020", "3", 17))
    assert "2020-09-17" == rfc3339.date(["2020", "09", 17])
    assert "2020-09-17" == rfc3339.date("2020", "09", 17)


def test_rfc3339_datetime():
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime("2020-03-17")
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime("2020/03/17")
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime("2020:03:17")
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime("2020_03_17")
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime("2020-03-17-12-34-56")
    assert "2020-03-17T12:34:00Z" == rfc3339.datetime("2020-03-17-12-34")
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime("2020:03:17:12:34:56")
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime("2020/03/17/12/34/56")
    assert "2020-03-17T12:34:00Z" == rfc3339.datetime("2020/03/17/12/34")
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime("2020_03_17_12_34_56")
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime("2020-03-17T12:34:56Z")
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime(date(2020, 3, 17))
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime(datetime(2020, 3, 17, 12, 34, 56))
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime((2020, 3, 17))
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime([2020, 3, 17])
    assert "2020-03-17T00:00:00Z" == rfc3339.datetime(2020, 3, 17)
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime((2020, 3, 17, 12, 34, 56))
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime([2020, 3, 17, 12, 34, 56])
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime(2020, 3, 17, 12, 34, 56)
    assert "2020-03-17T12:34:00Z" == rfc3339.datetime(2020, 3, 17, 12, 34)
    assert "2020-03-17T12:34:56Z" == rfc3339.datetime((2020, "3", 17, "12", "34", 56))
    assert "2020-09-17T12:34:56Z" == rfc3339.datetime([2020, "09", 17, "12", "34", 56])
    assert "2020-09-17T12:34:56Z" == rfc3339.datetime(2020, "09", "17", "12", "34", 56)


def test_rfc3339_normalize():
    assert "2020-03-17" == rfc3339.normalize("2020-03-17")
    assert "2020-03-17" == rfc3339.normalize("2020/03/17")
    assert "2020-03-17" == rfc3339.normalize("2020:03:17")
    assert "2020-03-17" == rfc3339.normalize("2020_03_17")
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize("2020-03-17-12-34-56")
    assert "2020-03-17T12:34:00Z" == rfc3339.normalize("2020-03-17-12-34")
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize("2020:03:17:12:34:56")
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize("2020/03/17/12/34/56")
    assert "2020-03-17T12:34:00Z" == rfc3339.normalize("2020/03/17/12/34")
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize("2020_03_17_12_34_56")
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize("2020-03-17T12:34:56Z")
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize("2020-03-17T12:34:56.44546546Z")
    assert "2020-03-17" == rfc3339.normalize(date(2020, 3, 17))
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize(datetime(2020, 3, 17, 12, 34, 56))
    assert "2020-03-17" == rfc3339.normalize((2020, 3, 17))
    assert "2020-03-17" == rfc3339.normalize([2020, 3, 17])
    assert "2020-03-17" == rfc3339.normalize(2020, 3, 17)
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize((2020, 3, 17, 12, 34, 56))
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize([2020, 3, 17, 12, 34, 56])
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize(2020, 3, 17, 12, 34, 56)
    assert "2020-03-17T12:00:00Z" == rfc3339.normalize(2020, 3, 17, 12)
    assert "2020-03-17T12:34:00Z" == rfc3339.normalize(2020, 3, 17, 12, 34)
    assert "2020-03-17T12:34:56Z" == rfc3339.normalize((2020, "3", 17, "12", "34", 56))
    assert "2020-09-17T12:34:56Z" == rfc3339.normalize([2020, "09", 17, "12", "34", 56])
    assert "2020-09-17T12:34:56Z" == rfc3339.normalize(2020, "09", "17", "12", "34", 56)


def test_rfc3339_datetime_dont_propagate_none():
    formatter = Rfc3339(propagate_none=False)
    assert formatter.datetime("2020-03-17") == "2020-03-17T00:00:00Z"
    assert formatter.date("2020-03-17") == "2020-03-17"
    with pytest.raises(ValueError):
        formatter.datetime(None)
    with pytest.raises(ValueError):
        formatter.date(None)


def test_rfc3339_datetime_propagate_none():
    formatter = Rfc3339(propagate_none=True)
    assert formatter.datetime("2020-03-17") == "2020-03-17T00:00:00Z"
    assert formatter.datetime(None) is None
    assert formatter.date("2020-03-17") == "2020-03-17"
    assert formatter.date(None) is None


def test_rfc3339_parse_date():
    assert rfc3339.parse_date("2011-12-13") == date(2011, 12, 13)
    # `datetime.strptime` does not require leading zeros for month, day, hour, minutes, seconds
    assert rfc3339.parse_date("0001-2-3") == date(1, 2, 3)


def test_rfc3339_parse_date_none():
    with pytest.raises(ValueError):
        rfc3339.parse_date(None)

    assert Rfc3339(propagate_none=True).parse_date(None) is None


@pytest.mark.parametrize("date", [
    "2011", "2011-12",
    "20111213", "2011/12/13", "2011:12:13",
    "11-12-14", "1-2-3",
    "2011-12-13T", "2011-12-13T14:15:16Z",
    "foobar",
])
def test_rfc3339_parse_date_invalid(date):
    with pytest.raises(ValueError):
        rfc3339.parse_date(date)


def test_rfc3339_parse_datetime():
    assert rfc3339.parse_datetime("2011-12-13T14:15:16Z") == datetime(2011, 12, 13, 14, 15, 16)
    # `datetime.strptime` is apparently case insensitive about non-placeholder bits
    assert rfc3339.parse_datetime("2011-12-13t14:15:16z") == datetime(2011, 12, 13, 14, 15, 16)
    # `datetime.strptime` does not require leading zeros for month, day, hour, minutes, seconds
    assert rfc3339.parse_datetime("0001-2-3T4:5:6Z") == datetime(1, 2, 3, 4, 5, 6)


def test_rfc3339_parse_datetime_none():
    with pytest.raises(ValueError):
        rfc3339.parse_datetime(None)

    assert Rfc3339(propagate_none=True).parse_datetime(None) is None


@pytest.mark.parametrize("date", [
    "2011", "2011-12", "2011-12-13", "2011-12-13T", "2011-12-13T14", "2011-12-13T14:15", "2011-12-13T14:15:16",
    "20111213141516",
    "2011/12/13T14:15:16Z", "2011-12-13T14-15-16Z", "2011:12:13T14:15:16Z",
    "1-2-3T4:5:6Z",
    "foobar",
])
def test_rfc3339_parse_datetime_invalid(date):
    with pytest.raises(ValueError):
        rfc3339.parse_datetime(date)


def test_rfc3339_parse_date_or_datetime():
    assert rfc3339.parse_date_or_datetime("2011-12-13") == date(2011, 12, 13)
    assert rfc3339.parse_date_or_datetime("0001-2-3") == date(1, 2, 3)
    assert rfc3339.parse_date_or_datetime("2011-12-13T14:15:16Z") == datetime(2011, 12, 13, 14, 15, 16)
    assert rfc3339.parse_date_or_datetime("2011-12-13t14:15:16z") == datetime(2011, 12, 13, 14, 15, 16)
    assert rfc3339.parse_date_or_datetime("0001-2-3T4:5:6Z") == datetime(1, 2, 3, 4, 5, 6)


def test_rfc3339_parse_date_or_datetime_none():
    with pytest.raises(ValueError):
        rfc3339.parse_date_or_datetime(None)

    assert Rfc3339(propagate_none=True).parse_date_or_datetime(None) is None


def test_dict_no_none_kwargs():
    assert dict_no_none() == {}
    assert dict_no_none(a=3) == {"a": 3}
    assert dict_no_none(a=3, b=0, c="foo") == {"a": 3, "b": 0, "c": "foo"}
    assert dict_no_none(a=3, b="", c="foo") == {"a": 3, "b": "", "c": "foo"}
    assert dict_no_none(a=3, b=None, c="foo") == {"a": 3, "c": "foo"}


def test_dict_no_none_args():
    assert dict_no_none() == {}
    assert dict_no_none({"a": 3}) == {"a": 3}
    assert dict_no_none({"a": 3, "b": 0, "c": "foo"}) == {"a": 3, "b": 0, "c": "foo"}
    assert dict_no_none({"a": 3, "b": "", "c": "foo"}) == {"a": 3, "b": "", "c": "foo"}
    assert dict_no_none({"a": 3, "b": None, "c": "foo"}) == {"a": 3, "c": "foo"}


@pytest.mark.parametrize(['input', 'expected'], [
    ((123,), 123),
    ((1, 2, 3), 1),
    ((0, 2, 3), 0),
    ((0.0, 2, 3), 0.0),
    ((None, 2, 3), 2),
    ((None, 'foo', 3), 'foo'),
    ((None, None, 3), 3),
    ((None, [], [1, 2]), []),
])
def test_first_not_none(input, expected):
    assert expected == first_not_none(*input)


def test_first_not_none_failures():
    with pytest.raises(ValueError):
        first_not_none()
    with pytest.raises(ValueError):
        first_not_none(None)
    with pytest.raises(ValueError):
        first_not_none(None, None)


@pytest.mark.parametrize(["input", "expected"], [
    (None, [None]),
    (123, [123]),
    ("abc", ["a", "b", "c"]),
    ([1, 2, "three"], [1, 2, "three"]),
    ((1, 2, "three"), [1, 2, "three"]),
    ({1: "a", 2: "b"}, [1, 2]),
    ({1, 2, 3, 3, 2}, [1, 2, 3]),
])
def test_ensure_list(input, expected):
    assert ensure_list(input) == expected


def test_ensure_dir_str(tmp_path):
    work_dir = str(tmp_path / "work/data/foo")
    assert not os.path.exists(work_dir)
    p = ensure_dir(work_dir)
    assert os.path.exists(work_dir)
    assert isinstance(p, pathlib.Path)
    assert p.exists()
    assert str(p) == str(work_dir)


def test_ensure_dir_pathlib(tmp_path):
    # Note: tmp_path might be pathlib2
    work_dir = pathlib.Path(str(tmp_path / "work/data/foo"))
    assert not work_dir.exists()
    p = ensure_dir(work_dir)
    assert work_dir.exists()
    assert isinstance(p, pathlib.Path)
    assert p.exists()
    assert str(p) == str(work_dir)


def test_get_temporal_extent():
    assert get_temporal_extent("2019-03-15") == ("2019-03-15", None)
    assert get_temporal_extent("2019-03-15", "2019-10-11") == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(["2019-03-15", "2019-10-11"]) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(("2019-03-15", "2019-10-11")) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(extent=["2019-03-15", "2019-10-11"]) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(extent=("2019-03-15", "2019-10-11")) == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(extent=(None, "2019-10-11")) == (None, "2019-10-11")
    assert get_temporal_extent(extent=("2019-03-15", None)) == ("2019-03-15", None)
    assert get_temporal_extent(start_date="2019-03-15", end_date="2019-10-11") == ("2019-03-15", "2019-10-11")
    assert get_temporal_extent(start_date="2019-03-15") == ("2019-03-15", None)
    assert get_temporal_extent(end_date="2019-10-11") == (None, "2019-10-11")


def test_context_timer_basic():
    with mock.patch.object(ContextTimer, "_clock", new=_fake_clock([3, 5, 8, 13])):
        with ContextTimer() as timer:
            assert timer.elapsed() == 2
            assert timer.elapsed() == 5
        assert timer.elapsed() == 10
        assert timer.elapsed() == 10


def test_context_timer_internals():
    with mock.patch.object(ContextTimer, "_clock", new=_fake_clock([3, 5, 8, 13])):
        ct = ContextTimer()
        assert ct.start is None
        assert ct.end is None
        with pytest.raises(RuntimeError):
            ct.elapsed()
        with ct as timer:
            assert timer is ct
            assert timer.start == 3
            assert timer.end is None
            assert timer.elapsed() is 2
            assert timer.elapsed() is 5
        assert timer.end == 13
        assert timer.elapsed() == 10
        assert timer.elapsed() == 10


def test_timing_logger_basic(caplog):
    caplog.set_level(logging.INFO)
    with TimingLogger("Testing"):
        logging.info("Hello world")

    pattern = re.compile(r'''
        .*Testing.+start.+\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}.*\n
        .*Hello\ world.*\n
        .*Testing.*end.+\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}.*elapsed.+[0-9.]+\n
        .*
        ''', re.VERBOSE | re.DOTALL)
    assert pattern.match(caplog.text)


class _Logger:
    """Logger stand-in for logging tests"""

    def __init__(self):
        self.logs = []

    def __call__(self, msg):
        self.logs.append(msg)


def _fake_clock(times: List[Union[int, datetime]] = None):
    # Trick to have a "time" function that returns different times in subsequent calls
    times = times or [datetime(2020, 3, 4, 5 + x, 2 * x, 1 + 3 * x, 1000) for x in range(0, 12)]
    return iter(times).__next__


def test_timing_logger_custom():
    logger = _Logger()
    timing_logger = TimingLogger("Testing", logger=logger)
    timing_logger._now = _fake_clock([
        datetime(2019, 12, 12, 10, 10, 10, 10000),
        datetime(2019, 12, 12, 11, 12, 13, 14141)
    ])

    with timing_logger:
        logger("Hello world")

    assert logger.logs == [
        "Testing: start 2019-12-12 10:10:10.010000",
        "Hello world",
        "Testing: end 2019-12-12 11:12:13.014141, elapsed 1:02:03.004141"
    ]


def test_timing_logger_context_return():
    logger = _Logger()
    timing_logger = TimingLogger("Testing", logger=logger)
    timing_logger._now = _fake_clock([
        datetime(2019, 12, 12, 10, 10, 10, 0),
        datetime(2019, 12, 12, 11, 12, 13, 0)
    ])

    with timing_logger as timer:
        logger("Hello world")

    assert timing_logger.elapsed.total_seconds() == 3723
    assert timer.elapsed.total_seconds() == 3723


def test_timing_logger_fail():
    logger = _Logger()
    timing_logger = TimingLogger("Testing", logger=logger)
    timing_logger._now = _fake_clock([
        datetime(2019, 12, 12, 10, 10, 10, 10000),
        datetime(2019, 12, 12, 11, 12, 13, 14141)
    ])

    try:
        with timing_logger:
            raise ValueError("Hello world")
    except ValueError:
        pass

    assert logger.logs == [
        "Testing: start 2019-12-12 10:10:10.010000",
        "Testing: fail 2019-12-12 11:12:13.014141, elapsed 1:02:03.004141"
    ]


def test_timing_logger_decorator():
    logger = _Logger()

    @TimingLogger("Decorated", logger=logger)
    def fun(x, y):
        logger("calculating {x} + {y}".format(x=x, y=y))
        return x + y

    with mock.patch.object(TimingLogger, '_now', new=_fake_clock()):
        fun(2, 3)
        fun(y=3, x=5)

    assert logger.logs == [
        'Decorated: start 2020-03-04 05:00:01.001000',
        'calculating 2 + 3',
        'Decorated: end 2020-03-04 06:02:04.001000, elapsed 1:02:03',
        'Decorated: start 2020-03-04 07:04:07.001000',
        'calculating 5 + 3',
        'Decorated: end 2020-03-04 08:06:10.001000, elapsed 1:02:03'
    ]


def test_timing_logger_method_decorator():
    logger = _Logger()

    class Foo:
        @TimingLogger("Decorated", logger=logger)
        def fun(self, x, y):
            logger("calculating {x} + {y}".format(x=x, y=y))
            return x + y

    f = Foo()
    with mock.patch.object(TimingLogger, '_now', new=_fake_clock()):
        f.fun(2, 3)
        f.fun(3, 5)

    assert logger.logs == [
        'Decorated: start 2020-03-04 05:00:01.001000',
        'calculating 2 + 3',
        'Decorated: end 2020-03-04 06:02:04.001000, elapsed 1:02:03',
        'Decorated: start 2020-03-04 07:04:07.001000',
        'calculating 3 + 5',
        'Decorated: end 2020-03-04 08:06:10.001000, elapsed 1:02:03'
    ]


def test_deep_get_dict():
    d = {
        "foo": "bar",
        "dims": {"x": 5, "y": {"amount": 3, "unit": "cm"}},
        "conversions": {4: 2, 6: {9: 3, 99: 7}},
    }
    assert deep_get(d, "foo") == "bar"
    with pytest.raises(DeepKeyError, match=re.escape("1 (from deep key ('foo', 1))")):
        deep_get(d, "foo", 1)
    with pytest.raises(DeepKeyError, match=re.escape("'bar' (from deep key ('bar',))")):
        deep_get(d, "bar")
    assert deep_get(d, "dims") == {"x": 5, "y": {"amount": 3, "unit": "cm"}}
    assert deep_get(d, "dims", "x") == 5
    with pytest.raises(DeepKeyError, match=re.escape("'unit' (from deep key ('dims', 'x', 'unit'))")):
        deep_get(d, "dims", "x", "unit")
    assert deep_get(d, "dims", "x", "unit", default="cm") == "cm"
    assert deep_get(d, "dims", "y", "amount") == 3
    assert deep_get(d, "dims", "y", "unit") == "cm"
    assert deep_get(d, "conversions", 4) == 2
    assert deep_get(d, "conversions", 6, 99) == 7


def test_deep_get_mixed():
    d = {
        "foo": (11, [222, 33], {"z": 42, -4: 44}),
        "bar": [{"a": [5, 8]}, {"b": ("ar", 6, 8)}]
    }
    assert deep_get(d, "foo", 0) == 11
    assert deep_get(d, "foo", 1) == [222, 33]
    assert deep_get(d, "foo", 1, 0) == 222
    assert deep_get(d, "foo", 1, 1) == 33
    assert deep_get(d, "foo", 2, "z") == 42
    assert deep_get(d, "foo", 2, -4) == 44
    with pytest.raises(DeepKeyError, match=re.escape("-4 (from deep key ('foo', -4))")):
        deep_get(d, "foo", -4)
    with pytest.raises(DeepKeyError, match=re.escape("10 (from deep key ('foo', 10))")):
        deep_get(d, "foo", 10)
    assert deep_get(d, "bar", 0, "a", 1) == 8
    assert deep_get(d, "bar", 1, "b", 0) == "ar"
    with pytest.raises(DeepKeyError, match=re.escape("2 (from deep key ('bar', 2, 22, 222))")):
        deep_get(d, "bar", 2, 22, 222)


@pytest.mark.parametrize(["init", "keys", "expected"], [
    ({}, ("foo",), {"foo": 42}),
    ({}, ("foo", "bar", "baz"), {"foo": {"bar": {"baz": 42}}}),
    ({"foo": {"x": "y"}}, ("foo", "bar"), {"foo": {"x": "y", "bar": 42}}),
    ({"foo": {"x": "y"}}, ("foo", "bar", "baz"), {"foo": {"x": "y", "bar": {"baz": 42}}}),
    ({"foo": [1, 2, 3]}, ("foo", 1), {"foo": [1, 42, 3]}),
    ({"foo": {1: "a", 2: "b"}}, ("foo", 1), {"foo": {1: 42, 2: "b"}}),
    ({"foo": [{"x": 1}, {"x": 2}, {"x": 3}]}, ("foo", 1, "x"), {"foo": [{"x": 1}, {"x": 42}, {"x": 3}]}),
    ({"foo": ({"x": 1}, {"x": 2}, {"x": 3})}, ("foo", 1, "x"), {"foo": ({"x": 1}, {"x": 42}, {"x": 3})}),
    ({"foo": [{"x": {}}, {"x": {}}]}, ("foo", 1, "x", "bar"), {"foo": [{"x": {}}, {"x": {"bar": 42}}]}),
    ({"foo": [{"x": {}}, {"x": {}}]}, ("foo", 1, "x", "y", "z"), {"foo": [{"x": {}}, {"x": {"y": {"z": 42}}}]}),
])
def test_deep_set_dict(init, keys, expected):
    d = init
    deep_set(d, *keys, value=42)
    assert d == expected


def test_get_user_config_dir():
    assert get_user_config_dir() == pathlib.Path(__file__).parent / "data/user_dirs/config/openeo-python-client"


def test_get_user_data_dir():
    assert get_user_data_dir() == pathlib.Path(__file__).parent / "data/user_dirs/data/openeo-python-client"


def test_guess_format():
    assert guess_format("./folder/file.nc") == "netCDF"
    assert guess_format("./folder/file.netcdf") == "netCDF"
    assert guess_format("./folder/file.tiff") == "GTiff"
    assert guess_format("./folder/file.gtiff") == "GTiff"
    assert guess_format("./folder/file.geotiff") == "GTiff"
    assert guess_format("/folder/file.png") == "PNG"
    assert guess_format("../folder/file.notaformat") == "NOTAFORMAT"


def test_legacy_alias_function(recwarn):
    def add(x, y):
        """Add x and y."""
        return x + y

    do_plus = legacy_alias(add, "do_plus")

    assert add.__doc__ == "Add x and y."
    assert do_plus.__doc__ == "Use of this legacy function is deprecated, use :py:func:`.add` instead."

    assert add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(DeprecationWarning, match="Call to deprecated function `do_plus`, use `add` instead."):
        res = do_plus(2, 3)
    assert res == 5


def test_legacy_alias_method(recwarn):
    class Foo:
        def add(self, x, y):
            """Add x and y."""
            return x + y

        do_plus = legacy_alias(add, "do_plus")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == "Use of this legacy method is deprecated, use :py:meth:`.add` instead."

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(DeprecationWarning, match="Call to deprecated method `do_plus`, use `add` instead."):
        res = Foo().do_plus(2, 3)
    assert res == 5


def test_legacy_alias_classmethod(recwarn):
    class Foo:
        @classmethod
        def add(cls, x, y):
            """Add x and y."""
            assert cls is Foo
            return x + y

        do_plus = legacy_alias(add, "do_plus")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == "Use of this legacy class method is deprecated, use :py:meth:`.add` instead."

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(DeprecationWarning, match="Call to deprecated class method `do_plus`, use `add` instead."):
        res = Foo().do_plus(2, 3)
    assert res == 5


def test_legacy_alias_staticmethod(recwarn):
    class Foo:
        @staticmethod
        def add(x, y):
            """Add x and y."""
            return x + y

        do_plus = legacy_alias(add, "do_plus")

    assert Foo.add.__doc__ == "Add x and y."
    assert Foo.do_plus.__doc__ == "Use of this legacy static method is deprecated, use :py:meth:`.add` instead."

    assert Foo().add(2, 3) == 5
    assert len(recwarn) == 0

    with pytest.warns(DeprecationWarning, match="Call to deprecated static method `do_plus`, use `add` instead."):
        res = Foo().do_plus(2, 3)
    assert res == 5


class TestLazyLoadCache:
    def test_basic(self):
        cache = LazyLoadCache()
        assert cache.get("foo", load=lambda: 4) == 4

    def test_load_once(self):
        # Trick to create a function that returns different results on each call
        load = iter([2, 3, 5, 8, 13]).__next__

        assert load() == 2
        assert load() == 3
        cache = LazyLoadCache()
        assert cache.get("foo", load) == 5
        assert cache.get("foo", load) == 5

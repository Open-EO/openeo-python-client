import logging
import os
import pathlib
import re
import unittest.mock as mock
from datetime import datetime, date
from typing import List

import pytest

from openeo.util import first_not_none, get_temporal_extent, TimingLogger, ensure_list, ensure_dir, dict_no_none, \
    deep_get, DeepKeyError, get_user_config_dir, get_user_data_dir, Rfc3339, rfc3339


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


def test_rfc3339_parse_datetime():
    assert rfc3339.parse_datetime("2011-12-13T14:15:16Z") == datetime(2011, 12, 13, 14, 15, 16)


def test_rfc3339_parse_datetime_none():
    with pytest.raises(ValueError):
        rfc3339.parse_datetime(None)

    assert Rfc3339(propagate_none=True).parse_datetime(None) is None


def test_dict_no_none():
    assert dict_no_none() == {}
    assert dict_no_none(a=3) == {"a": 3}
    assert dict_no_none(a=3, b=0, c="foo") == {"a": 3, "b": 0, "c": "foo"}
    assert dict_no_none(a=3, b="", c="foo") == {"a": 3, "b": "", "c": "foo"}
    assert dict_no_none(a=3, b=None, c="foo") == {"a": 3, "c": "foo"}


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


def _fake_clock(times: List[datetime] = None):
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
        fun(3, 5)

    assert logger.logs == [
        'Decorated: start 2020-03-04 05:00:01.001000',
        'calculating 2 + 3',
        'Decorated: end 2020-03-04 06:02:04.001000, elapsed 1:02:03',
        'Decorated: start 2020-03-04 07:04:07.001000',
        'calculating 3 + 5',
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


def test_get_user_config_dir():
    assert get_user_config_dir() == pathlib.Path(__file__).parent / "data/user_dirs/config/openeo-python-client"


def test_get_user_data_dir():
    assert get_user_data_dir() == pathlib.Path(__file__).parent / "data/user_dirs/data/openeo-python-client"

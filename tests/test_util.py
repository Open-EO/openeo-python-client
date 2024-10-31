import datetime as dt
import json
import logging
import os
import pathlib
import re
import unittest.mock as mock
from typing import List, Union

import pyproj
import pytest
import shapely.geometry

from openeo.capabilities import ComparableVersion
from openeo.util import (
    BBoxDict,
    ContextTimer,
    DeepKeyError,
    InvalidBBoxException,
    LazyLoadCache,
    Rfc3339,
    SimpleProgressBar,
    TimingLogger,
    clip,
    deep_get,
    deep_set,
    dict_no_none,
    ensure_dir,
    ensure_list,
    first_not_none,
    guess_format,
    normalize_crs,
    repr_truncate,
    rfc3339,
    str_truncate,
    to_bbox_dict,
    url_join,
)


class TestRfc3339:
    def test_date(self):
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
        assert "2020-03-17" == rfc3339.date(dt.date(2020, 3, 17))
        assert "2020-03-17" == rfc3339.date(dt.datetime(2020, 3, 17, 12, 34, 56))
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

    def test_datetime(self):
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
        assert "2020-03-17T00:00:00Z" == rfc3339.datetime(dt.date(2020, 3, 17))
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime(
            dt.datetime(2020, 3, 17, 12, 34, 56)
        )
        assert "2020-03-17T00:00:00Z" == rfc3339.datetime((2020, 3, 17))
        assert "2020-03-17T00:00:00Z" == rfc3339.datetime([2020, 3, 17])
        assert "2020-03-17T00:00:00Z" == rfc3339.datetime(2020, 3, 17)
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime((2020, 3, 17, 12, 34, 56))
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime([2020, 3, 17, 12, 34, 56])
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime(2020, 3, 17, 12, 34, 56)
        assert "2020-03-17T12:34:00Z" == rfc3339.datetime(2020, 3, 17, 12, 34)
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime(
            (2020, "3", 17, "12", "34", 56)
        )
        assert "2020-09-17T12:34:56Z" == rfc3339.datetime(
            [2020, "09", 17, "12", "34", 56]
        )
        assert "2020-09-17T12:34:56Z" == rfc3339.datetime(
            2020, "09", "17", "12", "34", 56
        )
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime(
            dt.datetime(2020, 3, 17, 12, 34, 56, tzinfo=None)
        )
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime(
            dt.datetime(2020, 3, 17, 12, 34, 56, tzinfo=dt.timezone.utc)
        )
        assert "2020-03-17T12:34:56Z" == rfc3339.datetime(
            dt.datetime(
                *(2020, 3, 17, 12, 34, 56),
                tzinfo=dt.timezone(offset=dt.timedelta(hours=0)),
            )
        )

    def test_normalize(self):
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
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize(
            "2020-03-17T12:34:56.44546546Z"
        )
        assert "2020-03-17" == rfc3339.normalize(dt.date(2020, 3, 17))
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize(
            dt.datetime(2020, 3, 17, 12, 34, 56)
        )
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize(
            dt.datetime(2020, 3, 17, 12, 34, 56, tzinfo=None)
        )
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize(
            dt.datetime(2020, 3, 17, 12, 34, 56, tzinfo=dt.timezone.utc)
        )
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize(
            dt.datetime(
                *(2020, 3, 17, 12, 34, 56),
                tzinfo=dt.timezone(offset=dt.timedelta(hours=0)),
            )
        )
        assert "2020-03-17" == rfc3339.normalize((2020, 3, 17))
        assert "2020-03-17" == rfc3339.normalize([2020, 3, 17])
        assert "2020-03-17" == rfc3339.normalize(2020, 3, 17)
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize((2020, 3, 17, 12, 34, 56))
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize([2020, 3, 17, 12, 34, 56])
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize(2020, 3, 17, 12, 34, 56)
        assert "2020-03-17T12:00:00Z" == rfc3339.normalize(2020, 3, 17, 12)
        assert "2020-03-17T12:34:00Z" == rfc3339.normalize(2020, 3, 17, 12, 34)
        assert "2020-03-17T12:34:56Z" == rfc3339.normalize(
            (2020, "3", 17, "12", "34", 56)
        )
        assert "2020-09-17T12:34:56Z" == rfc3339.normalize(
            [2020, "09", 17, "12", "34", 56]
        )
        assert "2020-09-17T12:34:56Z" == rfc3339.normalize(
            2020, "09", "17", "12", "34", 56
        )

    def test_datetime_dont_propagate_none(self):
        formatter = Rfc3339(propagate_none=False)
        assert formatter.datetime("2020-03-17") == "2020-03-17T00:00:00Z"
        assert formatter.date("2020-03-17") == "2020-03-17"
        with pytest.raises(ValueError):
            formatter.datetime(None)
        with pytest.raises(ValueError):
            formatter.date(None)

    def test_datetime_propagate_none(self):
        formatter = Rfc3339(propagate_none=True)
        assert formatter.datetime("2020-03-17") == "2020-03-17T00:00:00Z"
        assert formatter.datetime(None) is None
        assert formatter.date("2020-03-17") == "2020-03-17"
        assert formatter.date(None) is None

    def test_parse_date(self):
        assert rfc3339.parse_date("2011-12-13") == dt.date(2011, 12, 13)
        # `datetime.strptime` does not require leading zeros for month, day, hour, minutes, seconds
        assert rfc3339.parse_date("0001-2-3") == dt.date(1, 2, 3)

    def test_parse_date_none(self):
        with pytest.raises(ValueError):
            rfc3339.parse_date(None)

        assert Rfc3339(propagate_none=True).parse_date(None) is None

    @pytest.mark.parametrize(
        "date",
        [
            "2011",
            "2011-12",
            "20111213",
            "2011/12/13",
            "2011:12:13",
            "11-12-14",
            "1-2-3",
            "2011-12-13T",
            "2011-12-13T14:15:16Z",
            "foobar",
        ],
    )
    def test_parse_date_invalid(self, date):
        with pytest.raises(ValueError):
            rfc3339.parse_date(date)

    @pytest.mark.parametrize(
        ["input", "expected"],
        [
            ("2011-12-13T14:15:16Z", dt.datetime(2011, 12, 13, 14, 15, 16)),
            # `datetime.strptime` is apparently case-insensitive about non-placeholder bits
            ("2011-12-13t14:15:16z", dt.datetime(2011, 12, 13, 14, 15, 16)),
            # `datetime.strptime` does not require leading zeros for month, day, hour, minutes, seconds
            ("0001-2-3T4:5:6Z", dt.datetime(1, 2, 3, 4, 5, 6)),
            # Support for fractional seconds
            ("2011-12-13T14:15:16.789Z", dt.datetime(2011, 12, 13, 14, 15, 16, microsecond=789000)),
        ],
    )
    def test_parse_datetime(self, input, expected):
        assert rfc3339.parse_datetime(input) == expected

    @pytest.mark.parametrize(
        ["input", "expected"],
        [
            ("2011-12-13T14:15:16Z", dt.datetime(2011, 12, 13, 14, 15, 16, tzinfo=dt.timezone.utc)),
            # Support for fractional seconds
            (
                "2011-12-13T14:15:16.789876Z",
                dt.datetime(2011, 12, 13, 14, 15, 16, microsecond=789876, tzinfo=dt.timezone.utc),
            ),
        ],
    )
    def test_parse_datetime_with_timezone(self, input, expected):
        assert rfc3339.parse_datetime(input, with_timezone=True) == expected

    def test_parse_datetime_none(self):
        with pytest.raises(ValueError):
            rfc3339.parse_datetime(None)

        assert Rfc3339(propagate_none=True).parse_datetime(None) is None

    @pytest.mark.parametrize(
        "date",
        [
            "2011",
            "2011-12",
            "2011-12-13",
            "2011-12-13T",
            "2011-12-13T14",
            "2011-12-13T14:15",
            "2011-12-13T14:15:16",
            "20111213141516",
            "2011/12/13T14:15:16Z",
            "2011-12-13T14-15-16Z",
            "2011:12:13T14:15:16Z",
            "1-2-3T4:5:6Z",
            "foobar",
        ],
    )
    def test_parse_datetime_invalid(self, date):
        with pytest.raises(ValueError):
            rfc3339.parse_datetime(date)

    @pytest.mark.parametrize(
        ["input", "expected"],
        [
            ("2011-12-13", dt.date(2011, 12, 13)),
            ("0001-2-3", dt.date(1, 2, 3)),
            ("2011-12-13T14:15:16Z", dt.datetime(2011, 12, 13, 14, 15, 16)),
            ("2011-12-13t14:15:16z", dt.datetime(2011, 12, 13, 14, 15, 16)),
            ("2011-12-13T14:15:16.789Z", dt.datetime(2011, 12, 13, 14, 15, 16, microsecond=789000)),
            ("0001-2-3T4:5:6Z", dt.datetime(1, 2, 3, 4, 5, 6)),
        ],
    )
    def test_parse_date_or_datetime(self, input, expected):
        assert rfc3339.parse_date_or_datetime(input) == expected

    @pytest.mark.parametrize(
        ["input", "expected"],
        [
            ("2011-12-13T14:15:16Z", dt.datetime(2011, 12, 13, 14, 15, 16, tzinfo=dt.timezone.utc)),
            (
                "2011-12-13T14:15:16.789789Z",
                dt.datetime(2011, 12, 13, 14, 15, 16, microsecond=789789, tzinfo=dt.timezone.utc),
            ),
        ],
    )
    def test_parse_date_or_datetime_with_timezone(self, input, expected):
        assert rfc3339.parse_date_or_datetime(input, with_timezone=True) == expected

    def test_parse_date_or_datetime_none(self):
        with pytest.raises(ValueError):
            rfc3339.parse_date_or_datetime(None)

        assert Rfc3339(propagate_none=True).parse_date_or_datetime(None) is None

    def test_today(self, time_machine):
        time_machine.move_to("2023-02-10T12:34:56Z")
        assert rfc3339.today() == "2023-02-10"

    def test_utcnow(self, time_machine):
        time_machine.move_to("2023-02-10T12:34:56Z")
        assert rfc3339.utcnow() == "2023-02-10T12:34:56Z"
        time_machine.move_to("2023-02-10T12:34:56+03")
        assert rfc3339.utcnow() == "2023-02-10T09:34:56Z"


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
            assert timer.elapsed() == 2
            assert timer.elapsed() == 5
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


def _fake_clock(times: List[Union[int, dt.datetime]] = None):
    # Trick to have a "time" function that returns different times in subsequent calls
    times = times or [
        dt.datetime(2020, 3, 4, 5 + x, 2 * x, 1 + 3 * x, 1000) for x in range(0, 12)
    ]
    return iter(times).__next__


def test_timing_logger_custom():
    logger = _Logger()
    timing_logger = TimingLogger("Testing", logger=logger)
    timing_logger._now = _fake_clock(
        [
            dt.datetime(2019, 12, 12, 10, 10, 10, 10000),
            dt.datetime(2019, 12, 12, 11, 12, 13, 14141),
        ]
    )

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
    timing_logger._now = _fake_clock(
        [
            dt.datetime(2019, 12, 12, 10, 10, 10, 0),
            dt.datetime(2019, 12, 12, 11, 12, 13, 0),
        ]
    )

    with timing_logger as timer:
        logger("Hello world")

    assert timing_logger.elapsed.total_seconds() == 3723
    assert timer.elapsed.total_seconds() == 3723


def test_timing_logger_fail():
    logger = _Logger()
    timing_logger = TimingLogger("Testing", logger=logger)
    timing_logger._now = _fake_clock(
        [
            dt.datetime(2019, 12, 12, 10, 10, 10, 10000),
            dt.datetime(2019, 12, 12, 11, 12, 13, 14141),
        ]
    )

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


def test_guess_format():
    assert guess_format("./folder/file.nc") == "netCDF"
    assert guess_format("./folder/file.netcdf") == "netCDF"
    assert guess_format("./folder/file.tiff") == "GTiff"
    assert guess_format("./folder/file.gtiff") == "GTiff"
    assert guess_format("./folder/file.geotiff") == "GTiff"
    assert guess_format("/folder/file.png") == "PNG"
    assert guess_format("../folder/file.notaformat") == "NOTAFORMAT"

    assert guess_format("../folder/data") is None
    assert guess_format("../folder/data.tmp.nc") == "netCDF"


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


def test_str_truncate():
    assert str_truncate("hello world") == "hello world"
    assert str_truncate("hello world", width=10) == "hello w..."
    assert str_truncate("hello world", width=5) == "he..."
    assert str_truncate("hello world", width=3) == "..."
    assert str_truncate("hello world", width=1) == "."
    assert str_truncate("hello world", width=0) == ""
    assert str_truncate("hello world", width=-1) == ""
    assert str_truncate("hello world", width=-10) == ""
    assert str_truncate("hello world", width=10, ellipsis="<..>") == "hello <..>"


def test_repr_truncate_str():
    assert repr_truncate("hello world") == "'hello world'"
    assert repr_truncate("hello world", width=20) == "'hello world'"
    assert repr_truncate("hello world", width=12) == "'hello w...'"
    assert repr_truncate("hello world", width=7) == "'he...'"
    assert repr_truncate("hello world", width=6) == "'h...'"
    assert repr_truncate("hello world", width=5) == "'h..."
    assert repr_truncate("hello world", width=4) == "'..."
    assert repr_truncate("hello world", width=3) == "..."
    assert repr_truncate("hello world", width=2) == ".."
    assert repr_truncate("hello world", width=0) == ""
    assert repr_truncate("hello world", width=-1) == ""
    assert repr_truncate("hello world", width=-10) == ""
    assert repr_truncate("hello world", width=10, ellipsis="<->") == "'hello<->'"


def test_repr_truncate_generic():
    assert repr_truncate([1, 2, 3, 4, 5]) == "[1, 2, 3, 4, 5]"
    assert repr_truncate([1, 2, 3, 4, 5], width=10) == "[1, 2, ..."
    assert repr_truncate([1, 2, 3, 4, 5], width=5) == "[1..."
    assert repr_truncate([1, 2, 3, 4, 5], width=2) == ".."

    assert repr_truncate(["one", "two", "three"], width=10) == "['one',..."
    assert repr_truncate(ValueError("That's not right"), width=22) == 'ValueError("That\'s ...'


class TestBBoxDict:

    def test_init(self):
        assert BBoxDict(west=1, south=2, east=3, north=4) == {"west": 1, "south": 2, "east": 3, "north": 4}
        assert BBoxDict(west=1, south=2, east=3, north=4, crs="EPSG:4326") == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }
        assert BBoxDict(west=1, south=2, east=3, north=4, crs=4326) == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }

    def test_init_python_for_pyprojv331(self):
        """Extra test case that does not work with old pyproj versions that we get on python version 3.7 and below."""
        assert BBoxDict(west=1, south=2, east=3, north=4, crs="4326") == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }

    def test_repr(self):
        d = BBoxDict(west=1, south=2, east=3, north=4)
        assert repr(d) == "{'west': 1, 'south': 2, 'east': 3, 'north': 4}"

    def test_to_json(self):
        d = BBoxDict(west=1, south=2, east=3, north=4)
        assert json.dumps(d) == '{"west": 1, "south": 2, "east": 3, "north": 4}'

    def test_to_bbox_dict_from_sequence(self):
        assert to_bbox_dict([1, 2, 3, 4]) == {"west": 1, "south": 2, "east": 3, "north": 4}
        assert to_bbox_dict((1, 2, 3, 4)) == {"west": 1, "south": 2, "east": 3, "north": 4}
        assert to_bbox_dict([1, 2, 3, 4], crs=4326) == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }
        assert to_bbox_dict([1, 2, 3, 4], crs="EPSG:4326") == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }

    def test_to_bbox_dict_from_sequence_pyprojv331(self):
        """Extra test cases that do not work with old pyproj versions that we get on python version 3.7 and below."""
        assert to_bbox_dict([1, 2, 3, 4], crs="4326") == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }

    def test_to_bbox_dict_from_sequence_mismatch(self):
        with pytest.raises(InvalidBBoxException, match="Expected sequence with 4 items, but got 3."):
            to_bbox_dict([1, 2, 3])
        with pytest.raises(InvalidBBoxException, match="Expected sequence with 4 items, but got 5."):
            to_bbox_dict([1, 2, 3, 4, 5])

    def test_to_bbox_dict_from_dict(self):
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4}) == {
            "west": 1, "south": 2, "east": 3, "north": 4
        }
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326}) == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326"}) == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4}, crs="EPSG:4326") == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }
        assert to_bbox_dict(
            {
                "west": 1,
                "south": 2,
                "east": 3,
                "north": 4,
                "crs": "EPSG:4326",
                "color": "red",
                "other": "garbage",
            }
        ) == {"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326}

    def test_to_bbox_dict_from_dict_for_pyprojv331(self):
        """Extra test cases that do not work with old pyproj versions that we get on python version 3.7 and below."""
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "4326"}) == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4}, crs="4326") == {
            "west": 1,
            "south": 2,
            "east": 3,
            "north": 4,
            "crs": 4326,
        }

    def test_to_bbox_dict_from_dict_missing_field(self):
        with pytest.raises(InvalidBBoxException, match=re.escape("Missing bbox fields ['north', 'south', 'west']")):
            to_bbox_dict({"east": 3})

    def test_to_bbox_dict_multiple_crs(self):
        with pytest.raises(InvalidBBoxException, match="Two CRS values specified: EPSG:32631 and 4326"):
            _ = to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326}, crs="EPSG:32631")

    def test_to_bbox_dict_from_geometry(self):
        geometry = shapely.geometry.Polygon([(4, 2), (7, 4), (5, 8), (3, 3), (4, 2)])
        assert to_bbox_dict(geometry) == {"west": 3, "south": 2, "east": 7, "north": 8}


def test_url_join():
    assert url_join("http://d.test", "foo/bar") == "http://d.test/foo/bar"
    assert url_join("http://d.test/", "foo/bar") == "http://d.test/foo/bar"
    assert url_join("http://d.test", "/foo/bar") == "http://d.test/foo/bar"
    assert url_join("http://d.test/", "/foo/bar") == "http://d.test/foo/bar"


def test_clip():
    assert clip(-3, -2, 8) == -2
    assert clip(-1, -2, 8) == -1
    assert clip(-1, min=-2, max=8) == -1
    assert clip(1, -2, 8) == 1
    assert clip(8, -2, 8) == 8
    assert clip(18, -2, 8) == 8


class TestSimpleProgressBar:
    def test_basic(self):
        pgb = SimpleProgressBar()
        assert pgb.get(0.0) == "[--------------------------------------]"
        assert pgb.get(0.1) == "[####----------------------------------]"
        assert pgb.get(0.5) == "[###################-------------------]"
        assert pgb.get(1.0) == "[######################################]"

    def test_chars(self):
        pgb = SimpleProgressBar(bar="%", fill="_", left="[[", right=">>>")
        assert pgb.get(0.25) == "[[%%%%%%%%%__________________________>>>"

    def test_clip_and_overflow(self):
        pgb = SimpleProgressBar(bar="#%", fill="-_", left="[=", right="=]")
        assert pgb.get(0.0) == "[=------------------------------------=]"
        assert pgb.get(1.0) == "[=####################################=]"
        assert pgb.get(-0.5) == "[=------------------------------------=]"
        assert pgb.get(1.5) == "[=####################################=]"


class TestNormalizeCrs:
    WKT2_FOR_EPSG4326 = 'GEOGCRS["WGS 84",ENSEMBLE["World Geodetic System 1984 ensemble",MEMBER["World Geodetic System 1984 (Transit)"],MEMBER["World Geodetic System 1984 (G730)"],MEMBER["World Geodetic System 1984 (G873)"],MEMBER["World Geodetic System 1984 (G1150)"],MEMBER["World Geodetic System 1984 (G1674)"],MEMBER["World Geodetic System 1984 (G1762)"],MEMBER["World Geodetic System 1984 (G2139)"],ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ENSEMBLEACCURACY[2.0]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Horizontal component of 3D system."],AREA["World."],BBOX[-90,-180,90,180]],ID["EPSG",4326]]'
    WKT2_FOR_EPSG32631 = """
PROJCRS["WGS 84 / UTM zone 31N",
    BASEGEOGCRS["WGS 84",
        ENSEMBLE["World Geodetic System 1984 ensemble",
            MEMBER["World Geodetic System 1984 (Transit)"],
            MEMBER["World Geodetic System 1984 (G730)"],
            MEMBER["World Geodetic System 1984 (G873)"],
            MEMBER["World Geodetic System 1984 (G1150)"],
            MEMBER["World Geodetic System 1984 (G1674)"],
            MEMBER["World Geodetic System 1984 (G1762)"],
            MEMBER["World Geodetic System 1984 (G2139)"],
            ELLIPSOID["WGS 84",6378137,298.257223563,
                LENGTHUNIT["metre",1]],
            ENSEMBLEACCURACY[2.0]],
        PRIMEM["Greenwich",0,
            ANGLEUNIT["degree",0.0174532925199433]],
        ID["EPSG",4326]],
    CONVERSION["UTM zone 31N",
        METHOD["Transverse Mercator",
            ID["EPSG",9807]],
        PARAMETER["Latitude of natural origin",0,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8801]],
        PARAMETER["Longitude of natural origin",3,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8802]],
        PARAMETER["Scale factor at natural origin",0.9996,
            SCALEUNIT["unity",1],
            ID["EPSG",8805]],
        PARAMETER["False easting",500000,
            LENGTHUNIT["metre",1],
            ID["EPSG",8806]],
        PARAMETER["False northing",0,
            LENGTHUNIT["metre",1],
            ID["EPSG",8807]]],
    CS[Cartesian,2],
        AXIS["(E)",east,
            ORDER[1],
            LENGTHUNIT["metre",1]],
        AXIS["(N)",north,
            ORDER[2],
            LENGTHUNIT["metre",1]],
    USAGE[
        SCOPE["Engineering survey, topographic mapping."],
        AREA["Between 0°E and 6°E, northern hemisphere between equator and 84°N, onshore and offshore. Algeria. Andorra. Belgium. Benin. Burkina Faso. Denmark - North Sea. France. Germany - North Sea. Ghana. Luxembourg. Mali. Netherlands. Niger. Nigeria. Norway. Spain. Togo. United Kingdom (UK) - North Sea."],
        BBOX[0,0,84,6]],
    ID["EPSG",32631]]
"""

    @pytest.mark.parametrize(
        ["epsg_input", "expected"],
        [
            ("epsg:4326", 4326),
            ("EPSG:4326", 4326),
            ("Epsg:4326", 4326),
            ("epsg:32165", 32165),
            ("EPSG:32165", 32165),
            ("Epsg:32165", 32165),
            (4326, 4326),
            (32165, 32165),
            ("4326", 4326),
            ("32165", 32165),
            (None, None),
            ("", None),
            ({}, None),  # Should treat empty dict for PROJJSON the same way as "" or None
            # also likely to occur
            ("WGS84", 4326),
        ],
    )
    def test_normalize_crs_succeeds_with_correct_crses(self, epsg_input, expected):
        """Happy path, values that are allowed"""
        assert normalize_crs(epsg_input) == expected

    @pytest.mark.parametrize(
        ["epsg_input", "expected"],
        [
            ("epsg:4326", 4326),
            ("EPSG:4326", 4326),
            ("Epsg:4326", 4326),
            ("epsg:32165", 32165),
            ("EPSG:32165", 32165),
            ("Epsg:32165", 32165),
            (4326, 4326),
            (32165, 32165),
            ("4326", 4326),
            ("32165", 32165),
            (None, None),
            ("", None),
            ({}, None),  # Should treat empty dict for PROJJSON the same way as "" or None
            ('GEOGCRS["looks like WKT2"]', 'GEOGCRS["looks like WKT2"]'),
        ],
    )
    def test_normalize_crs_without_pyproj_succeeds_with_correct_crses(self, epsg_input, expected):
        """Happy path, values that are allowed"""
        assert normalize_crs(epsg_input, use_pyproj=False) == expected

    def test_normalize_crs_without_pyproj_accept_non_epsg_string(self, caplog):
        """Happy path, values that are allowed"""
        caplog.set_level(logging.WARNING)
        crs = self.WKT2_FOR_EPSG4326
        assert normalize_crs(crs, use_pyproj=False) == crs
        assert (
            """Assuming this is a valid WK2 CRS string: 'GEOGCRS["WGS 84",ENSEMBLE["World Geodetic System 1984 ensem...'"""
            in caplog.text
        )

    def test_normalize_crs_succeeds_with_wkt2_input(self):
        """Test can handle WKT2 strings.

        We need to support WKT2:
        See also https://github.com/Open-EO/openeo-processes/issues/58
        """
        assert normalize_crs(self.WKT2_FOR_EPSG32631) == 32631

    def test_normalize_crs_without_pyproj_succeeds_with_wkt2_input(self):
        assert normalize_crs(self.WKT2_FOR_EPSG32631, use_pyproj=False) == self.WKT2_FOR_EPSG32631

    PROJJSON_FOR_EPSG32631 = {
        "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
        "type": "ProjectedCRS",
        "name": "WGS 84 / UTM zone 31N",
        "base_crs": {
            "name": "WGS 84",
            "datum_ensemble": {
                "name": "World Geodetic System 1984 ensemble",
                "members": [
                    {"name": "World Geodetic System 1984 (Transit)", "id": {"authority": "EPSG", "code": 1166}},
                    {"name": "World Geodetic System 1984 (G730)", "id": {"authority": "EPSG", "code": 1152}},
                    {"name": "World Geodetic System 1984 (G873)", "id": {"authority": "EPSG", "code": 1153}},
                    {"name": "World Geodetic System 1984 (G1150)", "id": {"authority": "EPSG", "code": 1154}},
                    {"name": "World Geodetic System 1984 (G1674)", "id": {"authority": "EPSG", "code": 1155}},
                    {"name": "World Geodetic System 1984 (G1762)", "id": {"authority": "EPSG", "code": 1156}},
                    {"name": "World Geodetic System 1984 (G2139)", "id": {"authority": "EPSG", "code": 1309}},
                ],
                "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
                "accuracy": "2.0",
                "id": {"authority": "EPSG", "code": 6326},
            },
            "coordinate_system": {
                "subtype": "ellipsoidal",
                "axis": [
                    {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                    {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"},
                ],
            },
            "id": {"authority": "EPSG", "code": 4326},
        },
        "conversion": {
            "name": "UTM zone 31N",
            "method": {"name": "Transverse Mercator", "id": {"authority": "EPSG", "code": 9807}},
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": 0,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": 3,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "Scale factor at natural origin",
                    "value": 0.9996,
                    "unit": "unity",
                    "id": {"authority": "EPSG", "code": 8805},
                },
                {"name": "False easting", "value": 500000, "unit": "metre", "id": {"authority": "EPSG", "code": 8806}},
                {"name": "False northing", "value": 0, "unit": "metre", "id": {"authority": "EPSG", "code": 8807}},
            ],
        },
        "coordinate_system": {
            "subtype": "Cartesian",
            "axis": [
                {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": "metre"},
                {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": "metre"},
            ],
        },
        "scope": "Engineering survey, topographic mapping.",
        "area": "Between 0°E and 6°E, northern hemisphere between equator and 84°N, onshore and offshore. Algeria. Andorra. Belgium. Benin. Burkina Faso. Denmark - North Sea. France. Germany - North Sea. Ghana. Luxembourg. Mali. Netherlands. Niger. Nigeria. Norway. Spain. Togo. United Kingdom (UK) - North Sea.",
        "bbox": {"south_latitude": 0, "west_longitude": 0, "north_latitude": 84, "east_longitude": 6},
        "id": {"authority": "EPSG", "code": 32631},
    }

    def test_normalize_crs_succeeds_with_correct_projjson(
        self,
    ):
        json_str = json.dumps(self.PROJJSON_FOR_EPSG32631)

        # It should work with both a JSON string as well as the dict that
        # represents that same JSON.
        assert normalize_crs(json_str) == 32631
        assert normalize_crs(self.PROJJSON_FOR_EPSG32631) == 32631

    @pytest.mark.parametrize(
        ["epsg_input", "expected"],
        [
            ("+proj=latlon", 4326),
            ("+proj=utm +zone=31 +datum=WGS84 +units=m +no_defs", 32631),
        ],
    )
    def test_normalize_crs_succeeds_with_correct_projstring(self, epsg_input, expected):
        """These are more advanced inputs that pyproj should support, though
        the proj format is now discouraged, in favor of WKT2 and PROJJSON.

        See also https://github.com/Open-EO/openeo-processes/issues/58

        Contrary to WKT, it seems less likely that users would ask for these
        proj options. Hence a separate test.
        """
        assert normalize_crs(epsg_input) == expected

    @pytest.mark.parametrize(
        "epsg_input",
        ["doesnotexist", "unknownauthority:123", "4326.0", 0.0, 123.456, 4326.0, [], -4326, "-4326", {"foo": "bar"}],
    )
    @pytest.mark.parametrize("use_pyproj", [False, True])
    def test_normalize_crs_handles_incorrect_crs(self, epsg_input, use_pyproj):
        with pytest.raises(ValueError):
            normalize_crs(epsg_input, use_pyproj=use_pyproj)

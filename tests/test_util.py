import json
import logging
import os
import pathlib
import re
import unittest.mock as mock
import datetime as dt
from typing import List, Union

import shapely.geometry
import pytest

from openeo.util import first_not_none, get_temporal_extent, TimingLogger, ensure_list, ensure_dir, dict_no_none, \
    deep_get, DeepKeyError, Rfc3339, rfc3339, deep_set, \
    LazyLoadCache, guess_format, ContextTimer, str_truncate, to_bbox_dict, BBoxDict, repr_truncate, url_join


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

    def test_parse_datetime(self):
        assert rfc3339.parse_datetime("2011-12-13T14:15:16Z") == dt.datetime(
            2011, 12, 13, 14, 15, 16
        )
        # `datetime.strptime` is apparently case-insensitive about non-placeholder bits
        assert rfc3339.parse_datetime("2011-12-13t14:15:16z") == dt.datetime(
            2011, 12, 13, 14, 15, 16
        )
        # `datetime.strptime` does not require leading zeros for month, day, hour, minutes, seconds
        assert rfc3339.parse_datetime("0001-2-3T4:5:6Z") == dt.datetime(
            1, 2, 3, 4, 5, 6
        )
        # Timezone handling
        assert rfc3339.parse_datetime(
            "2011-12-13T14:15:16Z", with_timezone=True
        ) == dt.datetime(2011, 12, 13, 14, 15, 16, tzinfo=dt.timezone.utc)

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

    def test_parse_date_or_datetime(self):
        assert rfc3339.parse_date_or_datetime("2011-12-13") == dt.date(2011, 12, 13)
        assert rfc3339.parse_date_or_datetime("0001-2-3") == dt.date(1, 2, 3)
        assert rfc3339.parse_date_or_datetime("2011-12-13T14:15:16Z") == dt.datetime(
            2011, 12, 13, 14, 15, 16
        )
        assert rfc3339.parse_date_or_datetime("2011-12-13t14:15:16z") == dt.datetime(
            2011, 12, 13, 14, 15, 16
        )
        assert rfc3339.parse_date_or_datetime("0001-2-3T4:5:6Z") == dt.datetime(
            1, 2, 3, 4, 5, 6
        )
        assert rfc3339.parse_date_or_datetime(
            "2011-12-13T14:15:16Z", with_timezone=True
        ) == dt.datetime(2011, 12, 13, 14, 15, 16, tzinfo=dt.timezone.utc)

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
            "west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326",
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
        assert to_bbox_dict([1, 2, 3, 4], crs="EPSG:4326") == {
            "west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326",
        }

    def test_to_bbox_dict_from_sequence_mismatch(self):
        with pytest.raises(ValueError, match="Expected sequence with 4 items, but got 3."):
            to_bbox_dict([1, 2, 3])
        with pytest.raises(ValueError, match="Expected sequence with 4 items, but got 5."):
            to_bbox_dict([1, 2, 3, 4, 5])

    def test_to_bbox_dict_from_dict(self):
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4}) == {
            "west": 1, "south": 2, "east": 3, "north": 4
        }
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326"}) == {
            "west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326"
        }
        assert to_bbox_dict({"west": 1, "south": 2, "east": 3, "north": 4}, crs="EPSG:4326") == {
            "west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326",
        }
        assert to_bbox_dict({
            "west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326", "color": "red", "other": "garbage",
        }) == {
                   "west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326"
               }

    def test_to_bbox_dict_from_dict_missing_field(self):
        with pytest.raises(ValueError, match="but only found {'east'}"):
            to_bbox_dict({"east": 3})

    def test_to_bbox_dict_from_geometry(self):
        geometry = shapely.geometry.Polygon([(4, 2), (7, 4), (5, 8), (3, 3), (4, 2)])
        assert to_bbox_dict(geometry) == {"west": 3, "south": 2, "east": 7, "north": 8}


def test_url_join():
    assert url_join("http://d.test", "foo/bar") == "http://d.test/foo/bar"
    assert url_join("http://d.test/", "foo/bar") == "http://d.test/foo/bar"
    assert url_join("http://d.test", "/foo/bar") == "http://d.test/foo/bar"
    assert url_join("http://d.test/", "/foo/bar") == "http://d.test/foo/bar"

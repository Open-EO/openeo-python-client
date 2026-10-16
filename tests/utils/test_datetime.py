import datetime

import pytest

from openeo.utils.datetime import (
    DateTimeInterval,
    to_datetime_naive,
    to_datetime_utc,
    to_datetime_utc_unless_none,
)


@pytest.mark.parametrize(
    ["obj", "expected"],
    [
        ("2025-07-24", (2025, 7, 24, 0, 0, 0)),
        ("2025-07-24T12:34:56", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56.123", (2025, 7, 24, 12, 34, 56, 123000)),
        ("2025-07-24T12:34:56Z", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56+03", (2025, 7, 24, 9, 34, 56)),
        ("2025-07-24T12:34:56+03:30", (2025, 7, 24, 9, 4, 56)),
        ("2025-07-24T12:34:56-03", (2025, 7, 24, 15, 34, 56)),
        ("2025-07-24T12:34:56-03:30", (2025, 7, 24, 16, 4, 56)),
        (datetime.date(2025, 7, 24), (2025, 7, 24, 0, 0, 0)),
        (datetime.datetime(2025, 7, 24, 12, 34, 56), (2025, 7, 24, 12, 34, 56)),
        (
            datetime.datetime(2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=+2))),
            (2025, 7, 24, 10, 34, 56),
        ),
        (
            datetime.datetime(
                2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=-4, minutes=-30))
            ),
            (2025, 7, 24, 17, 4, 56),
        ),
    ],
)
def test_to_datetime_utc(obj, expected):
    actual = to_datetime_utc(obj)
    assert actual == datetime.datetime(*expected, tzinfo=datetime.timezone.utc)
    assert actual.tzinfo == datetime.timezone.utc


@pytest.mark.parametrize(
    ["obj", "expected"],
    [
        ("2025-07-24", (2025, 7, 24, 0, 0, 0)),
        ("2025-07-24T12:34:56", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56.123", (2025, 7, 24, 12, 34, 56, 123000)),
        ("2025-07-24T12:34:56Z", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56+03", (2025, 7, 24, 9, 34, 56)),
        ("2025-07-24T12:34:56+03:30", (2025, 7, 24, 9, 4, 56)),
        ("2025-07-24T12:34:56-03", (2025, 7, 24, 15, 34, 56)),
        ("2025-07-24T12:34:56-03:30", (2025, 7, 24, 16, 4, 56)),
        (datetime.date(2025, 7, 24), (2025, 7, 24, 0, 0, 0)),
        (datetime.datetime(2025, 7, 24, 12, 34, 56), (2025, 7, 24, 12, 34, 56)),
        (
            datetime.datetime(2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=+2))),
            (2025, 7, 24, 10, 34, 56),
        ),
        (
            datetime.datetime(
                2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=-4, minutes=-30))
            ),
            (2025, 7, 24, 17, 4, 56),
        ),
    ],
)
def test_to_datetime_naive(obj, expected):
    actual = to_datetime_naive(obj)
    assert actual == datetime.datetime(*expected, tzinfo=None)
    assert actual.tzinfo is None


@pytest.mark.parametrize(
    ["obj", "expected"],
    [
        (None, None),
        ("2025-07-24", datetime.datetime(2025, 7, 24, 0, 0, 0, tzinfo=datetime.timezone.utc)),
        (
            datetime.datetime(2025, 7, 24, 12, 34, 56),
            datetime.datetime(2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_to_datetime_utc_unless_none(obj, expected):
    assert to_datetime_utc_unless_none(obj) == expected


class TestDateTimeInterval:
    def test_init_basic_date(self):
        interval = DateTimeInterval("2026-09-10", "2026-11-12")
        assert interval.start == datetime.datetime(2026, 9, 10, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert interval.end == datetime.datetime(2026, 11, 12, 0, 0, 0, tzinfo=datetime.timezone.utc)

    def test_init_basic_invalid(self):
        with pytest.raises(ValueError):
            _ = DateTimeInterval("2026-11-12", "2026-09-10")

    def test_init_basic_datetime(self):
        interval = DateTimeInterval("2026-09-10T12:34:45", "2026-11-12T11:22:33")
        assert interval.start == datetime.datetime(2026, 9, 10, 12, 34, 45, tzinfo=datetime.timezone.utc)
        assert interval.end == datetime.datetime(2026, 11, 12, 11, 22, 33, tzinfo=datetime.timezone.utc)

    def test_init_basic_datetime_z(self):
        interval = DateTimeInterval("2026-09-10T12:34:45Z", "2026-11-12T11:22:33Z")
        assert interval.start == datetime.datetime(2026, 9, 10, 12, 34, 45, tzinfo=datetime.timezone.utc)
        assert interval.end == datetime.datetime(2026, 11, 12, 11, 22, 33, tzinfo=datetime.timezone.utc)

    def test_init_basic_datetime_timezone(self):
        interval = DateTimeInterval("2026-09-10T11:11:11+02:00", "2026-11-12T22:22:22-1:30")
        assert interval.start == datetime.datetime(2026, 9, 10, 9, 11, 11, tzinfo=datetime.timezone.utc)
        assert interval.end == datetime.datetime(2026, 11, 12, 23, 52, 22, tzinfo=datetime.timezone.utc)

    @pytest.mark.parametrize(
        ["interval", "expected"],
        [
            (DateTimeInterval("2026-09-10", "2026-09-20"), True),
            (DateTimeInterval("2026-09-10T00:00:00", "2026-09-20T00:00:00"), True),
            (DateTimeInterval("2026-09-10T01:00:00+01:00", "2026-09-20T00:00:00"), True),
            (DateTimeInterval("2026-09-10T01:00:00", "2026-09-20T00:00:00"), False),
        ],
    )
    def test_equality(self, interval, expected):
        ref = DateTimeInterval("2026-09-10", "2026-09-20")
        assert (ref == interval) == expected
        assert (interval == ref) == expected

    def test_equality_end_inclusive(self):
        d1 = "2026-09-10"
        d2 = "2026-09-20"
        assert DateTimeInterval(d1, d2) == DateTimeInterval(d1, d2, end_inclusive=True)
        assert DateTimeInterval(d1, d2) != DateTimeInterval(d1, d2, end_inclusive=False)
        assert DateTimeInterval(d1, d2, end_inclusive=False) == DateTimeInterval(d1, d2, end_inclusive=False)

    @pytest.mark.parametrize(
        ["given", "expected"],
        [
            (("2026-09-10", "2026-11-12"), DateTimeInterval("2026-09-10", "2026-11-12")),
            (("2026-09-10", None), DateTimeInterval("2026-09-10", None)),
            (["2026-09-10", "2026-11-12"], DateTimeInterval("2026-09-10", "2026-11-12")),
            (None, None),
            ("foobar", None),
            (["2026-09-10"], None),
            (["2026-09-10", "2026-11-12", "2026-12-01"], None),
            ([{"2026": "09-10"}, {"2026": "11-12"}], None),
            ({"2026": "09-10"}, None),
        ],
    )
    def test_try_from(self, given, expected):
        assert DateTimeInterval.try_from_tuple(given) == expected
    def test_as_tuple(self):
        interval = DateTimeInterval("2026-09-10", "2026-11-12T11:22:33Z")
        assert interval.as_tuple() == (
            datetime.datetime(2026, 9, 10, 0, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2026, 11, 12, 11, 22, 33, tzinfo=datetime.timezone.utc),
        )

    def test_as_isoformat(self):
        interval = DateTimeInterval("2026-09-10", "2026-11-12T11:22:33Z")
        assert interval.as_isoformat() == ("2026-09-10T00:00:00+00:00", "2026-11-12T11:22:33+00:00")

    def test_as_rfc3339(self):
        interval = DateTimeInterval("2026-09-10", "2026-11-12T11:22:33Z")
        assert interval.as_rfc3339() == ("2026-09-10T00:00:00Z", "2026-11-12T11:22:33Z")

    @pytest.mark.parametrize(
        ["interval1", "interval2", "expected"],
        [
            # Closed intervals
            (("2026-09-10", "2026-11-12"), ("2025-05-05", "2026-06-06"), None),
            (("2026-09-10", "2026-11-12"), ("2026-06-06", "2026-10-10"), DateTimeInterval("2026-09-10", "2026-10-10")),
            (("2026-09-10", "2026-11-12"), ("2026-10-01", "2026-10-10"), DateTimeInterval("2026-10-01", "2026-10-10")),
            (("2026-09-10", "2026-11-12"), ("2026-10-10", "2027-07-07"), DateTimeInterval("2026-10-10", "2026-11-12")),
            (("2026-09-10", "2026-11-12"), ("2027-07-07", "2027-07-27"), None),
            # Half-open intervals
            (("2026-09-10", None), ("2025-05-05", "2026-06-06"), None),
            (("2026-09-10", None), ("2026-06-06", "2026-10-10"), DateTimeInterval("2026-09-10", "2026-10-10")),
            (("2026-09-10", None), ("2026-10-01", "2026-10-10"), DateTimeInterval("2026-10-01", "2026-10-10")),
            ((None, "2026-11-12"), ("2025-05-05", "2026-06-06"), DateTimeInterval("2025-05-05", "2026-06-06")),
            ((None, "2026-11-12"), ("2026-10-10", "2027-07-07"), DateTimeInterval("2026-10-10", "2026-11-12")),
            ((None, "2026-11-12"), ("2027-07-07", "2027-07-27"), None),
            # More half-open intervals
            (("2026-06-06", None), ("2025-05-05", None), DateTimeInterval("2026-06-06", None)),
            (("2026-06-06", None), (None, "2025-05-05"), None),
            ((None, "2026-06-06"), ("2025-05-05", None), DateTimeInterval("2025-05-05", "2026-06-06")),
            ((None, "2026-06-06"), (None, "2025-05-05"), DateTimeInterval(None, "2025-05-05")),
            (("2026-06-06", None), (None, None), DateTimeInterval("2026-06-06", None)),
            ((None, "2026-06-06"), (None, None), DateTimeInterval(None, "2026-06-06")),
            ((None, None), (None, None), DateTimeInterval(None, None)),
        ],
    )
    def test_intersection(self, interval1, interval2, expected):
        interval1 = DateTimeInterval(*interval1)
        interval2 = DateTimeInterval(*interval2)
        assert interval1.intersection(interval2) == expected
        assert interval2.intersection(interval1) == expected

    @pytest.mark.parametrize(
        ["interval1", "interval2", "expected"],
        [
            (
                DateTimeInterval("2026-08-20", "2026-09-01"),
                DateTimeInterval("2026-09-01", "2026-09-30"),
                DateTimeInterval("2026-09-01", "2026-09-01"),
            ),
            (
                DateTimeInterval("2026-08-20", "2026-09-01", end_inclusive=False),
                DateTimeInterval("2026-09-01", "2026-09-30"),
                None,
            ),
            (
                DateTimeInterval("2026-08-20", "2026-09-01", end_inclusive=False),
                DateTimeInterval("2026-08-25", "2026-09-30"),
                DateTimeInterval("2026-08-25", "2026-09-01", end_inclusive=False),
            ),
        ],
    )
    def test_intersection_end_inclusive(self, interval1, interval2, expected):
        assert interval1.intersection(interval2) == expected
        assert interval2.intersection(interval1) == expected

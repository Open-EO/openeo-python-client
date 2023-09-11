import datetime as dt
from typing import Union

import pytest

from openeo.dates import (
    get_temporal_extent,
    _convert_abbreviated_temporal_extent,
    _convert_abbreviated_date,
    _type_of_date_string,
    _TypeOfDateString,
)


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
    assert get_temporal_extent(start_date="2019") == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent(start_date="2019-01") == ("2019-01-01", "2019-02-01")
    assert get_temporal_extent(start_date="2019-11") == ("2019-11-01", "2019-12-01")
    assert get_temporal_extent(start_date="2019-12") == ("2019-12-01", "2020-01-01")


def test_get_temporal_extent_with_abbreviated_dates():
    assert get_temporal_extent("2019") == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent("2019-03") == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent("2019", "2019") == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent("2019", "2021") == ("2019-01-01", "2022-01-01")
    assert get_temporal_extent("2019-03", "2019-03") == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent("2019-03", "2019-04") == ("2019-03-01", "2019-05-01")
    assert get_temporal_extent("2019-03", "2021-06") == ("2019-03-01", "2021-07-01")
    assert get_temporal_extent("2019-03", "2021-06-12") == ("2019-03-01", "2021-06-12")
    assert get_temporal_extent((None, "2019")) == (None, "2020-01-01")
    assert get_temporal_extent((None, "2019-10")) == (None, "2019-11-01")
    assert get_temporal_extent((None, "2019-10-11")) == (None, "2019-10-11")

    assert get_temporal_extent(extent=["2019", None]) == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent(extent=["2019", "2019"]) == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent(extent=["2019-03", None]) == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent(extent=["2019-03", "2019-03"]) == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent(extent=["2019", "2019"]) == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent(extent=["2019", "2021"]) == ("2019-01-01", "2022-01-01")
    assert get_temporal_extent(extent=["2019-03", "2019-03"]) == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent(extent=["2019-03", "2019-04"]) == ("2019-03-01", "2019-05-01")
    assert get_temporal_extent(extent=["2019-03", "2021-06"]) == ("2019-03-01", "2021-07-01")
    assert get_temporal_extent(extent=["2019-03", "2021-06-12"]) == ("2019-03-01", "2021-06-12")
    assert get_temporal_extent(extent=[None, "2019"]) == (None, "2020-01-01")
    assert get_temporal_extent(extent=[None, "2019-10"]) == (None, "2019-11-01")
    assert get_temporal_extent(extent=[None, "2019-10-11"]) == (None, "2019-10-11")

    assert get_temporal_extent(start_date="2019") == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent(start_date="2019", end_date="2019") == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent(start_date="2019-03") == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent(start_date="2019-03", end_date="2019-03") == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent(start_date="2019", end_date="2019") == ("2019-01-01", "2020-01-01")
    assert get_temporal_extent(start_date="2019", end_date="2021") == ("2019-01-01", "2022-01-01")
    assert get_temporal_extent(start_date="2019-03", end_date="2019-03") == ("2019-03-01", "2019-04-01")
    assert get_temporal_extent(start_date="2019-03", end_date="2019-04") == ("2019-03-01", "2019-05-01")
    assert get_temporal_extent(start_date="2019-03", end_date="2021-06") == ("2019-03-01", "2021-07-01")
    assert get_temporal_extent(start_date="2019-03", end_date="2021-06-12") == ("2019-03-01", "2021-06-12")
    assert get_temporal_extent(end_date="2019") == (None, "2020-01-01")
    assert get_temporal_extent(end_date="2019-10") == (None, "2019-11-01")
    assert get_temporal_extent(end_date="2019-10-11") == (None, "2019-10-11")


class TestConvertAbbreviatedTemporalExtent:
    @pytest.mark.parametrize(
        ["date_input", "expected_start", "expected_end"],
        [
            ("2023", dt.date(2023, 1, 1), dt.date(2024, 1, 1)),
            ("1999", dt.date(1999, 1, 1), dt.date(2000, 1, 1)),
            ("2023-03", dt.date(2023, 3, 1), dt.date(2023, 4, 1)),
            ("2023/03", dt.date(2023, 3, 1), dt.date(2023, 4, 1)),
            ("2023-01", dt.date(2023, 1, 1), dt.date(2023, 2, 1)),
            ("2023/01", dt.date(2023, 1, 1), dt.date(2023, 2, 1)),
            ("2022-12", dt.date(2022, 12, 1), dt.date(2023, 1, 1)),
            ("2022/12", dt.date(2022, 12, 1), dt.date(2023, 1, 1)),
            ("2022-11", dt.date(2022, 11, 1), dt.date(2022, 12, 1)),
            ("2022/11", dt.date(2022, 11, 1), dt.date(2022, 12, 1)),
            ("2022-12-31", "2022-12-31", None),
            ("2022/12/31", "2022/12/31", None),
            ("2022-11-30", "2022-11-30", None),
            ("2022/11/30", "2022/11/30", None),
            ("2022-12-31T12:33:05Z", "2022-12-31T12:33:05Z", None),
            (dt.date(2022, 11, 1), dt.date(2022, 11, 1), None),
            (dt.datetime(2022, 11, 1, 15, 30, 00), dt.datetime(2022, 11, 1, 15, 30, 00), None),
        ],
    )
    def test_convert_abbreviated_temporal_extent(self, date_input: str, expected_start: dt.date, expected_end: dt.date):
        actual_start, actual_end = _convert_abbreviated_temporal_extent(date_input)
        assert actual_start == expected_start
        assert actual_end == expected_end

    @pytest.mark.parametrize(
        ["end_date", "expected_end"],
        [
            ("2023", dt.date(2024, 1, 1)),
            ("1999", dt.date(2000, 1, 1)),
            ("2023-03", dt.date(2023, 4, 1)),
            ("2023/03", dt.date(2023, 4, 1)),
            ("2023-01", dt.date(2023, 2, 1)),
            ("2023/01", dt.date(2023, 2, 1)),
            ("2022-12", dt.date(2023, 1, 1)),
            ("2022/12", dt.date(2023, 1, 1)),
            ("2022-11", dt.date(2022, 12, 1)),
            ("2022/11", dt.date(2022, 12, 1)),
            ("2022-12-31", "2022-12-31"),
            ("2022/12/31", "2022/12/31"),
            ("2022-11-30", "2022-11-30"),
            ("2022/11/30", "2022/11/30"),
            ("2022-12-31T12:33:05Z", "2022-12-31T12:33:05Z"),
            (dt.date(2022, 11, 1), dt.date(2022, 11, 1)),
            (dt.datetime(2022, 11, 1, 15, 30, 00), dt.datetime(2022, 11, 1, 15, 30, 00)),
        ],
    )
    def test_convert_abbreviated_temporal_extent_with_only_end(self, end_date: str, expected_end: dt.date):
        actual_start, actual_end = _convert_abbreviated_temporal_extent(None, end_date)
        assert actual_end == expected_end
        assert actual_start is None

    @pytest.mark.parametrize(
        ["start_date", "end_date", "expected_start", "expected_end"],
        [
            ("2023", "2023", dt.date(2023, 1, 1), dt.date(2024, 1, 1)),
            ("2023", "2025", dt.date(2023, 1, 1), dt.date(2026, 1, 1)),
            ("1999", "1999", dt.date(1999, 1, 1), dt.date(2000, 1, 1)),
            ("1999", "2000", dt.date(1999, 1, 1), dt.date(2001, 1, 1)),
            ("2023-03", "2023-03", dt.date(2023, 3, 1), dt.date(2023, 4, 1)),
            ("2023-03", "2023-04", dt.date(2023, 3, 1), dt.date(2023, 5, 1)),
            ("2023-03", "2023-08", dt.date(2023, 3, 1), dt.date(2023, 9, 1)),
            ("2023-03", "2023-12", dt.date(2023, 3, 1), dt.date(2024, 1, 1)),
            ("2023-01", "2023-01", dt.date(2023, 1, 1), dt.date(2023, 2, 1)),
            ("2023-01", "2023-02", dt.date(2023, 1, 1), dt.date(2023, 3, 1)),
            ("2023-01", "2023-06", dt.date(2023, 1, 1), dt.date(2023, 7, 1)),
            ("2023-01", "2023-12", dt.date(2023, 1, 1), dt.date(2024, 1, 1)),
            ("2022-12", "2022-12", dt.date(2022, 12, 1), dt.date(2023, 1, 1)),
            ("2022-12", "2023-01", dt.date(2022, 12, 1), dt.date(2023, 2, 1)),
            ("2022-12", "2023-09", dt.date(2022, 12, 1), dt.date(2023, 10, 1)),
            ("2022-12", "2023-12", dt.date(2022, 12, 1), dt.date(2024, 1, 1)),
            ("2022-12", "2024-01", dt.date(2022, 12, 1), dt.date(2024, 2, 1)),
            ("2022-11", "2022-11", dt.date(2022, 11, 1), dt.date(2022, 12, 1)),
            ("2022-11", "2022-12", dt.date(2022, 11, 1), dt.date(2023, 1, 1)),
            ("2022-11", "2023-01", dt.date(2022, 11, 1), dt.date(2023, 2, 1)),
            ("2022-12-31", "2023", "2022-12-31", dt.date(2024, 1, 1)),
            ("2022-11-30", "2022", "2022-11-30", dt.date(2023, 1, 1)),
            ("2022", "2023-12-31", dt.date(2022, 1, 1), "2023-12-31"),
            ("2022", "2023-11-30", dt.date(2022, 1, 1), "2023-11-30"),
            ("2022-12-31T12:33:05Z", "2023", "2022-12-31T12:33:05Z", dt.date(2024, 1, 1)),
            ("2022-12-31T12:33:05Z", "2023-06", "2022-12-31T12:33:05Z", dt.date(2023, 7, 1)),
            (dt.date(2022, 11, 1), "2023", dt.date(2022, 11, 1), dt.date(2024, 1, 1)),
            (dt.date(2022, 11, 1), "2023-01", dt.date(2022, 11, 1), dt.date(2023, 2, 1)),
            (dt.datetime(2022, 11, 1, 15, 30, 00), "2023", dt.datetime(2022, 11, 1, 15, 30, 00), dt.date(2024, 1, 1)),
            (
                dt.datetime(2022, 11, 1, 15, 30, 00),
                "2023-01",
                dt.datetime(2022, 11, 1, 15, 30, 00),
                dt.date(2023, 2, 1),
            ),
        ],
    )
    def test_convert_abbreviated_temporal_extent_with_start_and_end(
        self, start_date: str, end_date: str, expected_start: dt.date, expected_end: dt.date
    ):
        actual_start, actual_end = _convert_abbreviated_temporal_extent(start_date, end_date)
        assert actual_start == expected_start
        assert actual_end == expected_end


@pytest.mark.parametrize(
    "date_input",
    [
        "foobar",
        "20-22-12-31",
        "2022/12/31/aa1/bb/cc",
        "20-2--12",
        "2021-2--12",
        "2021-1-1-",
        "2021-2-",
        "-2021-2",
    ],
)
def test_convert_abbreviated_temporal_extent_raises_valueerror(date_input: Union[str, dt.date, dt.datetime]):
    # It should raise error for an incorrect start date.
    with pytest.raises(ValueError):
        _convert_abbreviated_temporal_extent(date_input)

    # It should also raise error for an incorrect end date.
    with pytest.raises(ValueError):
        _convert_abbreviated_temporal_extent("2000", date_input)


class TestConvertAbbreviatedDate:
    @pytest.mark.parametrize(
        ["date_input", "expected"],
        [
            ("2023", dt.date(2023, 1, 1)),
            ("1999", dt.date(1999, 1, 1)),
            ("2023-03", dt.date(2023, 3, 1)),
            ("2023/03", dt.date(2023, 3, 1)),
            ("2023-01", dt.date(2023, 1, 1)),
            ("2023/01", dt.date(2023, 1, 1)),
            ("2022-12", dt.date(2022, 12, 1)),
            ("2022/12", dt.date(2022, 12, 1)),
            ("2022-11", dt.date(2022, 11, 1)),
            ("2022/11", dt.date(2022, 11, 1)),
        ],
    )
    def test_convert_abbreviated_date_does_convert_years_and_months(self, date_input: str, expected: dt.date):
        assert _convert_abbreviated_date(date_input) == expected

    @pytest.mark.parametrize(
        ["date_input", "expected"],
        [
            ("2022-12-31", "2022-12-31"),
            ("2022/12/31", "2022/12/31"),
            ("2022-11-30", "2022-11-30"),
            ("2022/11/30", "2022/11/30"),
            ("2022-12-31T12:33:05Z", "2022-12-31T12:33:05Z"),
        ],
    )
    def test_convert_abbreviated_date_does_not_convert_full_days_and_datetimes(
        self, date_input: str, expected: dt.date
    ):
        """Days and datetimes should not be converted."""
        assert _convert_abbreviated_date(date_input) == expected

    @pytest.mark.parametrize(
        "date_input",
        [
            "foobar",
            "20-22-12-31",
            "2022/12/31/aa1/bb/cc",
            "20-2--12",
            "2021-2--12",
            "2021-1-1-",
            "2021-2-",
            "-2021-2",
        ],
    )
    def test_convert_abbreviated_date_raises_valueerror(self, date_input: Union[str, dt.date, dt.datetime]):
        with pytest.raises(ValueError):
            _convert_abbreviated_date(date_input)

    @pytest.mark.parametrize(
        "date_input",
        [
            # _convert_abbreviated_date is not intended to handle Dates and datetimes, only strings
            dt.date(2022, 11, 1),
            dt.datetime(2022, 11, 1, 15, 30, 00),
            # And any other types are just plain wrong:
            2000,  # Could represent a year but there is no intention to deal with this.
            False,
            {},  # "falsey" value, but it is not None
            (),  # "falsey" value, but it is not None
        ],
    )
    def test_convert_abbreviated_date_raises_typeerror(self, date_input: any):
        """_convert_abbreviated_date only handles strings and None, anything else is forbidden."""
        with pytest.raises(TypeError):
            _convert_abbreviated_date(date_input)


class TestTypeOfDateString:
    @pytest.mark.parametrize(
        ["date", "expected"],
        [
            ("2023", _TypeOfDateString.YEAR),
            ("1999", _TypeOfDateString.YEAR),
            ("2023-03", _TypeOfDateString.MONTH),
            ("2023/03", _TypeOfDateString.MONTH),
            ("2023-01", _TypeOfDateString.MONTH),
            ("2023/01", _TypeOfDateString.MONTH),
            ("2022-12", _TypeOfDateString.MONTH),
            ("2022/12", _TypeOfDateString.MONTH),
            ("2022-11", _TypeOfDateString.MONTH),
            ("2022/11", _TypeOfDateString.MONTH),
            ("2022-12-31", _TypeOfDateString.DAY),
            ("2022/12/31", _TypeOfDateString.DAY),
            ("2022-11-30", _TypeOfDateString.DAY),
            ("2022/11/30", _TypeOfDateString.DAY),
            ("2022-12-31T12:33:05Z", _TypeOfDateString.DATETIME),
        ],
    )
    def test_type_of_date(self, date, expected):
        assert _type_of_date_string(date) == expected

    @pytest.mark.parametrize(
        "date",
        [
            dt.date(2022, 11, 1),
            dt.datetime(2022, 11, 1, 15, 30, 00),
            None,
            [],
        ],
    )
    def test_type_of_date_string_raises_typeerror(self, date):
        with pytest.raises(TypeError):
            _type_of_date_string(date)

    @pytest.mark.parametrize(
        "date",
        [
            "foobar",
            "20-22-12-31",
            "2022/12/31/aa1/bb/cc",
            "20-2--12",
            "2021-2--12",
            "2021-1-1-",
            "2021-2-",
            "-2021-2",
        ],
    )
    def test_type_of_date_string_detects_invalid_strings(self, date):
        assert _type_of_date_string(date) == _TypeOfDateString.INVALID

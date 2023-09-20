import datetime as dt
from typing import Union

import pytest

from openeo.dates import _convert_abbreviated_date, _type_of_date_string, _TypeOfDateString, get_temporal_extent


class TestGetTemporalExtent:
    def test_args_basic(self):
        """get_temporal_extent(*args)"""
        # Base case
        assert get_temporal_extent("2019-03-15", "2019-10-11") == ("2019-03-15", "2019-10-11")
        # Half open-ended extents
        assert get_temporal_extent("2019-03-15") == ("2019-03-15", None)
        assert get_temporal_extent("2019-03-15", None) == ("2019-03-15", None)
        assert get_temporal_extent(None, "2019-10-11") == (None, "2019-10-11")

        # Fully open-ended
        assert get_temporal_extent(None, None) == (None, None)
        assert get_temporal_extent(None) == (None, None)
        assert get_temporal_extent() == (None, None)

    def test_args_too_much(self):
        with pytest.raises(ValueError):
            _ = get_temporal_extent("2019-03-15", "2019-10-11", "2019-12-12")

    @pytest.mark.parametrize(
        ["input", "expected"],
        [
            (("2019-03-15", "2019-10-11"), ("2019-03-15", "2019-10-11")),
            (("2019-03-15", None), ("2019-03-15", None)),
            (("2019-03-15",), ("2019-03-15", None)),
            ((None, "2019-10-11"), (None, "2019-10-11")),
            ((None, None), (None, None)),
            ((None,), (None, None)),
            ((), (None, None)),
        ],
    )
    def test_args_single_sequence(self, input, expected):
        """Just pas a single tuple/list with one or two items"""
        assert get_temporal_extent(list(input)) == expected
        assert get_temporal_extent(tuple(input)) == expected

    def test_args_single_sequence_too_much(self):
        with pytest.raises(ValueError):
            _ = get_temporal_extent(["2019-03-15", "2019-10-11", "2019-12-12"])
        with pytest.raises(ValueError):
            _ = get_temporal_extent(("2019-03-15", "2019-10-11", "2019-12-12"))

    def test_args_shorthand_year(self):
        assert get_temporal_extent("2019", "2021") == ("2019-01-01", "2021-01-01")
        assert get_temporal_extent("2019") == ("2019-01-01", None)
        assert get_temporal_extent("2019", None) == ("2019-01-01", None)
        assert get_temporal_extent(None, "2021") == (None, "2021-01-01")

    def test_args_shorthand_month(self):
        assert get_temporal_extent("2019-03", "2019-07") == ("2019-03-01", "2019-07-01")
        assert get_temporal_extent("2019-03") == ("2019-03-01", None)
        assert get_temporal_extent("2019-03", None) == ("2019-03-01", None)
        assert get_temporal_extent(None, "2019-07") == (None, "2019-07-01")

    @pytest.mark.parametrize(
        ["input", "expected"],
        [
            (("2019", "2021"), ("2019-01-01", "2021-01-01")),
            (("2019-02", "2020-03"), ("2019-02-01", "2020-03-01")),
            (("2019", None), ("2019-01-01", None)),
            (("2019",), ("2019-01-01", None)),
            (("2019-03",), ("2019-03-01", None)),
            ((None, "2021"), (None, "2021-01-01")),
            ((None, None), (None, None)),
            ((None,), (None, None)),
            ((), (None, None)),
        ],
    )
    def test_args_single_sequence_shorthand(self, input, expected):
        """Just pas a single tuple/list with one or two items"""
        assert get_temporal_extent(tuple(input)) == expected
        assert get_temporal_extent(list(input)) == expected

    def test_args_something_else(self):
        assert get_temporal_extent({"da": "te"}, {3: 4}, convertor=lambda x: x) == ({"da": "te"}, {3: 4})

    def test_start_and_end_basic(self):
        """get_temporal_extent(start_date=..., end_date=...)"""
        assert get_temporal_extent(start_date="2019-03-15", end_date="2019-10-11") == ("2019-03-15", "2019-10-11")
        assert get_temporal_extent(start_date="2019-03-15") == ("2019-03-15", None)
        assert get_temporal_extent(start_date="2019-03-15", end_date=None) == ("2019-03-15", None)
        assert get_temporal_extent(end_date="2019-10-11") == (None, "2019-10-11")
        assert get_temporal_extent(start_date=None, end_date="2019-10-11") == (None, "2019-10-11")
        assert get_temporal_extent(start_date=None, end_date=None) == (None, None)
        assert get_temporal_extent(start_date=None) == (None, None)
        assert get_temporal_extent(end_date=None) == (None, None)
        assert get_temporal_extent() == (None, None)

    def test_start_and_end_shorthand_year(self):
        assert get_temporal_extent(start_date="2019") == ("2019-01-01", None)
        assert get_temporal_extent(start_date="2019", end_date="2021") == ("2019-01-01", "2021-01-01")
        assert get_temporal_extent(end_date="2021") == (None, "2021-01-01")

    def test_start_and_end_shorthand_month(self):
        assert get_temporal_extent(start_date="2019-03") == ("2019-03-01", None)
        assert get_temporal_extent(start_date="2019-03", end_date="2019-07") == ("2019-03-01", "2019-07-01")
        assert get_temporal_extent(end_date="2019-07") == (None, "2019-07-01")

    def test_start_and_end_something_else(self):
        assert get_temporal_extent(
            start_date={"da": "te"},
            end_date={3: 4},
            convertor=lambda x: x,
        ) == ({"da": "te"}, {3: 4})

    @pytest.mark.parametrize(
        ["extent", "expected"],
        [
            (["2019-03-15", "2019-10-11"], ("2019-03-15", "2019-10-11")),
            (["2019-03-15", None], ("2019-03-15", None)),
            ([None, "2019-10-11"], (None, "2019-10-11")),
            ([None, None], (None, None)),
        ],
    )
    def test_extent_basic(self, extent, expected):
        """get_temporal_extent(extent=...)"""
        assert get_temporal_extent(extent=list(extent)) == expected
        assert get_temporal_extent(extent=tuple(extent)) == expected

    @pytest.mark.parametrize(
        ["extent", "expected"],
        [
            (["2019", "2021"], ("2019-01-01", "2021-01-01")),
            (["2019", None], ("2019-01-01", None)),
            ([None, "2021"], (None, "2021-01-01")),
            ([None, None], (None, None)),
        ],
    )
    def test_extent_sequence_shorthand_year(self, extent, expected):
        """get_temporal_extent(extent=...)"""
        assert get_temporal_extent(extent=list(extent)) == expected
        assert get_temporal_extent(extent=tuple(extent)) == expected

    @pytest.mark.parametrize(
        ["extent", "expected"],
        [
            (["2019-03", "2019-07"], ("2019-03-01", "2019-07-01")),
            (["2019-03", None], ("2019-03-01", None)),
            ([None, "2019-07"], (None, "2019-07-01")),
            ([None, None], (None, None)),
        ],
    )
    def test_extent_sequence_shorthand_month(self, extent, expected):
        """get_temporal_extent(extent=...)"""
        assert get_temporal_extent(extent=list(extent)) == expected
        assert get_temporal_extent(extent=tuple(extent)) == expected

    @pytest.mark.parametrize(
        ["extent", "expected"],
        [
            ("2019", ("2019-01-01", "2020-01-01")),
            ("2019-03", ("2019-03-01", "2019-04-01")),
            ("2019-12", ("2019-12-01", "2020-01-01")),
            ("2019-03-05", ("2019-03-05", "2019-03-06")),
            ("2019-12-31", ("2019-12-31", "2020-01-01")),
        ],
    )
    def test_extent_single_string_shorthand(self, extent, expected):
        """get_temporal_extent(extent=...)"""
        assert get_temporal_extent(extent=extent) == expected

    def test_extent_something_else(self):
        assert get_temporal_extent(extent=({"da": "te"}, {3: 4}), convertor=lambda x: x) == ({"da": "te"}, {3: 4})

    def test_no_arguments(self):
        assert get_temporal_extent() == (None, None)

    def test_multiple_argument_modes(self):
        with pytest.raises(ValueError, match="At most one of .* should be provided"):
            _ = get_temporal_extent("2021-02-03", end_date="2021-03-04")
        with pytest.raises(ValueError, match="At most one of .* should be provided"):
            _ = get_temporal_extent("2021-02-03", extent=["2021-03-04", "2021-03-07"])
        with pytest.raises(ValueError, match="At most one of .* should be provided"):
            _ = get_temporal_extent(start_date="2021-02-03", extent=["2021-03-04", "2021-03-07"])


class TestConvertAbbreviatedDate:
    """
    Unit tests for _convert_abbreviated_date
    """

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
    def test_shorthand_conversion(self, date_input: str, expected: dt.date):
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
    def test_preserve_date_strings(self, date_input: str, expected: dt.date):
        """Full date/datetime formatted stringsshould not be converted."""
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
    def test_invalid_string_raises_value_error(self, date_input: Union[str, dt.date, dt.datetime]):
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
    def test_preserve_other(self, date_input: any):
        assert _convert_abbreviated_date(date_input) == date_input


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

from __future__ import annotations

import datetime as dt
import re
from enum import Enum
from typing import Any, Tuple, Union

from openeo.util import rfc3339


def get_temporal_extent(
    *args,
    start_date: Union[str, dt.date, None, Any] = None,
    end_date: Union[str, dt.date, None, Any] = None,
    extent: Union[list, tuple, str, None] = None,
    convertor=rfc3339.normalize,
) -> Tuple[Union[str, None], Union[str, None]]:
    """
    Helper to derive a date extent from various call forms:

        >>> get_temporal_extent("2019-01-01")
        ("2019-01-01", None)
        >>> get_temporal_extent("2019-01-01", "2019-05-15")
        ("2019-01-01", "2019-05-15")
        >>> get_temporal_extent(["2019-01-01", "2019-05-15"])
        ("2019-01-01", "2019-05-15")
        >>> get_temporal_extent(start_date="2019-01-01", end_date="2019-05-15"])
        ("2019-01-01", "2019-05-15")
        >>> get_temporal_extent(extent=["2019-01-01", "2019-05-15"])
        ("2019-01-01", "2019-05-15")

    It also supports resolving year/month shorthand notation (rounding down to first day of year or month):

        >>> get_temporal_extent("2019")
        ("2019-01-01", None)
        >>> get_temporal_extent(start_date="2019-02", end_date="2019-03"])
        ("2019-02-01", "2019-03-01")

    And even interpretes extents given as a single string:

        >>> get_temporal_extent(extent="2021")
        ("2021-01-01", "2022-01-01")

    """
    if (bool(len(args) > 0) + bool(start_date or end_date) + bool(extent)) > 1:
        raise ValueError("At most one of `*args`, `start_date/end_date`, or `extent` should be provided")
    if args:
        # Convert positional `*args` to `start_date`/`end_date` argument
        if len(args) == 2:
            start_date, end_date = args
        elif len(args) == 1:
            arg = args[0]
            if isinstance(arg, (list, tuple)):
                if len(args) > 2:
                    raise ValueError(f"Unable to handle {args} as a temporal extent")
                start_date, end_date = tuple(arg) + (None,) * (2 - len(arg))
            else:
                start_date, end_date = arg, None
        else:
            raise ValueError(f"Unable to handle {args} as a temporal extent")
    elif extent:
        if isinstance(extent, (list, tuple)) and len(extent) == 2:
            start_date, end_date = extent
        elif isinstance(extent, str):
            # Special case: extent is given as a single string (e.g. "2021" for full year extent
            # or "2021-04" for full month extent): convert that to the appropriate extent tuple.
            start_date, end_date = _convert_abbreviated_date(extent), _get_end_of_time_slot(extent)
        else:
            raise ValueError(f"Unable to handle {extent} as a temporal extent")
    start_date = _convert_abbreviated_date(start_date)
    end_date = _convert_abbreviated_date(end_date)
    return convertor(start_date) if start_date else None, convertor(end_date) if end_date else None


class _TypeOfDateString(Enum):
    """Enum that denotes which kind of date a string represents.

    This is an internal helper class, not intended to be public.
    """

    INVALID = 0  # It was neither of the options below
    YEAR = 1
    MONTH = 2
    DAY = 3
    DATETIME = 4


_REGEX_DAY = re.compile(r"^(\d{4})[:/_-](\d{2})[:/_-](\d{2})$")
_REGEX_MONTH = re.compile(r"^(\d{4})[:/_-](\d{2})$")
_REGEX_YEAR = re.compile(r"^\d{4}$")


def _get_end_of_time_slot(date: str) -> Union[dt.date, str]:
    """Calculate the end of a left-closed period: the first day after a year or month."""
    if not isinstance(date, str):
        return date

    date_converted = _convert_abbreviated_date(date)
    granularity = _type_of_date_string(date)
    if granularity == _TypeOfDateString.YEAR:
        return dt.date(date_converted.year + 1, 1, 1)
    elif granularity == _TypeOfDateString.MONTH:
        if date_converted.month == 12:
            return dt.date(date_converted.year + 1, 1, 1)
        else:
            return dt.date(date_converted.year, date_converted.month + 1, 1)
    elif granularity == _TypeOfDateString.DAY:
        # TODO: also support day granularity in _convert_abbreviated_date so that we don't need ad-hoc parsing here
        return dt.date(*(int(x) for x in _REGEX_DAY.match(date).group(1, 2, 3))) + dt.timedelta(days=1)
    else:
        # Don't convert: it is a day or datetime.
        return date


def _convert_abbreviated_date(
    date: Union[str, dt.date, dt.datetime, Any],
) -> Union[str, dt.date, dt.datetime, Any]:
    """
    Helper function to convert a year- or month-abreviated strings (e.g. "2021" or "2021-03") into a date
    (first day of the corresponding period). Other values are returned as original.

    :param date: some kind of date representation:

        - A string, formatted "yyyy", "yyyy-mm", "yyyy-mm-dd" or with even more granularity
        - Any other type (e.g. ``datetime.date``, ``datetime.datetime``, a parameter, ...)

    :return:
        If input was a string representing a year or a month:
        a ``datetime.date`` that represents the first day of that year or month.
        Otherwise, the original version is returned as-is.

    :raises ValueError:
        when ``date`` was a string but not recognized as a date representation

    Examples
    --------

    >>> # For year and month: "round down" to fist day:
    >>> _convert_abbreviated_date("2021")
    datetime.date(2021, 1, 1)
    >>> _convert_abbreviated_date("2022-08")
    datetime.date(2022, 8, 1)

    >>> # Preserve other values
    >>> _convert_abbreviated_date("2022-08-15")
    '2022-08-15'
    """
    if not isinstance(date, str):
        return date

    # TODO: avoid double regex matching? Once in _type_of_date_string and once here.
    type_of_date = _type_of_date_string(date)
    if type_of_date == _TypeOfDateString.INVALID:
        raise ValueError(
            f"The value of date='{date}' does not represent any of: "
            + "a year ('yyyy'), a year + month ('yyyy-dd'), a date, or a datetime."
        )

    if type_of_date in [_TypeOfDateString.DATETIME, _TypeOfDateString.DAY]:
        # TODO: also convert these to `date` or `datetime` for more internal consistency.
        return date

    if type_of_date == _TypeOfDateString.MONTH:
        match_month = _REGEX_MONTH.match(date)
        year = int(match_month.group(1))
        month = int(match_month.group(2))
    else:
        year = int(date)
        month = 1

    return dt.date(year, month, 1)


def _type_of_date_string(date: str) -> _TypeOfDateString:
    """Returns which type of date the string represents: year, month, day or datetime."""

    if not isinstance(date, str):
        raise TypeError("date must be a string")

    try:
        rfc3339.parse_datetime(date)
        return _TypeOfDateString.DATETIME
    except ValueError:
        pass

    # Using a separate and stricter regular expressions to detect day, month,
    # or year. Having a regex that only matches one type of period makes it
    # easier to check it is effectively only a year, or only a month,
    # but not a day. Datetime strings are more complex so we use rfc3339 to
    # check whether or not it represents a datetime.
    match_day = _REGEX_DAY.match(date)
    match_month = _REGEX_MONTH.match(date)
    match_year = _REGEX_YEAR.match(date)

    if match_day:
        return _TypeOfDateString.DAY
    if match_month:
        return _TypeOfDateString.MONTH
    if match_year:
        return _TypeOfDateString.YEAR

    return _TypeOfDateString.INVALID

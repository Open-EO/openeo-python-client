from __future__ import annotations

import datetime
import datetime as dt
import re
from enum import Enum
from typing import Union, Tuple, Any

from openeo.util import rfc3339


def get_temporal_extent(
    *args,
    start_date: Union[str, dt.datetime, dt.date] = None,
    end_date: Union[str, dt.datetime, dt.date] = None,
    extent: Union[list, tuple] = None,
    convertor=rfc3339.normalize,
) -> Tuple[Union[str, None], Union[str, None]]:
    """
    Helper to derive a date extent from from various call forms:

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
    """
    if args:
        assert start_date is None and end_date is None and extent is None
        if len(args) == 2:
            start_date, end_date = args
        elif len(args) == 1:
            arg = args[0]
            if isinstance(arg, (list, tuple)):
                start_date, end_date = arg
            else:
                start_date, end_date = arg, None
        else:
            raise ValueError("Unable to handle {a!r} as a date range".format(a=args))
    elif extent:
        assert start_date is None and end_date is None
        if isinstance(extent, str):
            start_date, end_date = extent, None
        else:
            start_date, end_date = extent
    start_date, end_date = _convert_abbreviated_temporal_extent(start_date, end_date)
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


def _convert_abbreviated_temporal_extent(start_date: Any, end_date: Any = None) -> Tuple[Any, Any]:
    """Convert strings representing entire years or months into a normalized date range.

    The result is a 2-tuple ``(start, end)`` that represents the period as a
    half-open, left-closed interval, i.e. the end date is not included in the period.

    This function should ONLY convert string values into a datetime.date when
    they are clearly abbreviations, that is what is is intended for.
    In all other cases leave the original start_date or end_date as it was.

    Keep in mind that this function is called by ``get_temporal_extent``,
    and that in general the start date and end date can be None, though usually
    not at the same time.
    Because we cannot know what the missing start or end means in every context,
    it is the calling function's job to handle that.
    We only handle the case where we know the end date is implicitly determined
    by the start date because the start date is a shorthand for a year or a month.

    Also ``get_temporal_extent`` uses a callable to convert the dates, in order
    to deal with things like ProcessGraph parameters.
    That means we need to accept those other types but we must return those
    values unchanged.

    :param start_date:

        - Typically a string that represents either a year, a year + month, a day,
            or a datetime, and it always indicates the *beginning* of that period.
        - Other data types allowed are a ``datetime.date`` and ``datetime.datetime``,
            and in that case we return the those values unchanged.
            Similarly, strings that represent a date or datetime are not processed
            any further are returned unchanged as the start or end of the tuple.
        - Since callers may try to deal with even more types in their own way
            we do except any type, but return them unchanged and so the caller
            can convert them.

        - Allowed string formats are:
            - For year: "yyyy"
            - For year + month: "yyyy-mm"
                Some other separators than "-" technically work but they are discouraged.
            - For date and datetime you must follow the RFC 3339 format. See also: class ``Rfc3339``

    :return:
        The result is a 2-tuple of the form ``(start, end)`` that represents
        the period as a half-open, left-close interval, i.e. the end date is the
        first day that is no longer part of the time slot.

    :raises ValueError:
        when start_date was a string but not recognized as either a year,
        a month, a date, or datetime. The format was invalid.

    Examples
    --------

    >>> import datetime
    >>>
    >>> # 1. Year: use all data from the start of 2021 to the end of 2021.
    >>> string_to_temporal_extent("2021")
    (datetime.date(2021, 1, 1), datetime.date(2022, 1, 1))
    >>>
    >>> # 2. Year + month: all data from the start of August 2022 to the end of August 2022.
    >>> string_to_temporal_extent("2022-08")
    (datetime.date(2022, 8, 1), datetime.date(2022, 9, 1))
    >>>
    >>> # 3. We received a full date 2022-08-15:
    >>> # In this case we should not process start_date. The calling function/method must
    >>> # handle end date, depending on what an interval with an open end means for the caller.
    >>> # See for example how ``get_temporal_extent`` handles this.
    >>> string_to_temporal_extent("2022-08-15")
    ('2022-08-15', None)
    >>>
    >>> # 4. Similar to 3), but with a datetime.date instead of a string containing a date.
    >>> string_to_temporal_extent(datetime.date(2022, 8, 15))
    (datetime.date(2022, 8, 15), None)
    >>>
    >>> # 5. Similar to 3) & 4), but with a datetime.datetime instance.
    >>> string_to_temporal_extent(datetime.datetime(2022, 8, 15, 0, 0))
    (datetime.datetime(2022, 8, 15, 0, 0), None)
    """

    # Exclude case where nothing was specified to keep code below simpler.
    # We want to check for None explicitly here, don't accept any other values
    # that evaluate to False.
    if start_date is None and end_date is None:
        return None, None

    # Only strings can be abbreviated dates. We don't touch it if it is any other type.
    start_date_converted = None
    if start_date is not None:
        if isinstance(start_date, str):
            start_date_converted = _convert_abbreviated_date(start_date)

    # If an end date was specified we should use it, and convert it when it is an abbreviation.
    # But when only a start was specified, we derive the end date from the start date.
    if end_date:
        result_start_date = start_date_converted or start_date
        return result_start_date, _get_end_of_time_slot(end_date)

    # Only the start date was specified, when we reach this point.
    if isinstance(start_date_converted, dt.date):
        # start_date was effectively converted => derive end date from it.
        result_end_date = _get_end_of_time_slot(start_date)
        return start_date_converted, result_end_date
    else:
        # start_date was not abbreviated, it is a day or datetime:
        # Therefor we should not derive the end date from start date because
        # the caller should handle this case themselves.
        return start_date, None


def _get_end_of_time_slot(date: str) -> Union[dt.date, str]:
    """Calculate the end of a left-closed period: the first day after a year or month."""
    if not isinstance(date, str):
        return date

    date_converted = _convert_abbreviated_date(date)
    type_start_date = _type_of_date_string(date)
    if type_start_date == _TypeOfDateString.YEAR:
        return dt.date(date_converted.year + 1, 1, 1)
    elif type_start_date == _TypeOfDateString.MONTH:
        if date_converted.month == 12:
            return dt.date(date_converted.year + 1, 1, 1)
        else:
            return dt.date(date_converted.year, date_converted.month + 1, 1)
    else:
        # Don't convert: it is a day or datetime.
        return date


def _convert_abbreviated_date(
    date: str,
) -> Union[dt.date, str]:
    """Helper function to convert a string into a date when it is an abbreviation for an entire year or month.

    The intent of this function is to only convert values into a datetime.date
    when they are clearly abbreviations, and in all other cases return the original
    value of date.

    :param date:

        - Typically a string that represents either a year, a year + month, a day,
            or a datetime, and it always indicates the *beginning* of that period.
        - Strings that represent a day or a datetime are not processed.
            In that case we return the original value of ``date`` unchanged.
        - Any other type than str raises a TypeError.

        - Allowed string formats are:
            - For year: "yyyy"
            - For year + month: "yyyy-mm"
                Some other separators than "-" technically work but they are discouraged.
            - For date and datetime you must follow the RFC 3339 format. See also: class ``Rfc3339``

    :return:
        If it was a string representing a year or a month:
        a datetime.date that represents the first day of that year or month.

        If the string represents a day, or a datetime than the original string will be returned.

    :raises TypeError:
        when date is not type ``str``

    :raises ValueError:
        when ``date`` was a string but not recognized as either a year,
        a month, a date, or datetime.

    Examples
    --------

    >>> import datetime
    >>>
    >>> # 1. Year: use all data from the start of 2021 to the end of 2021.
    >>> _convert_abbreviated_date("2021")
    datetime.date(2021, 1, 1)
    >>>
    >>> # 2. Year + month: all data from the start of August 2022 to the end of August 2022.
    >>> _convert_abbreviated_date("2022-08")
    datetime.date(2022, 8, 1)
    >>>
    >>> # 3. We received a full date 2022-08-15:
    >>> # In this case we should not process start_date. The calling function/method must
    >>> # handle end date, depending on what an interval with an open end means for the caller.
    >>> # See for example how ``get_temporal_extent`` handles this.
    >>> _convert_abbreviated_date("2022-08-15")
    '2022-08-15'
    """
    if not isinstance(date, str):
        raise TypeError("date must be a string")

    type_of_date = _type_of_date_string(date)
    if type_of_date == _TypeOfDateString.INVALID:
        raise ValueError(
            f"The value of date='{date}' does not represent any of: "
            + "a year ('yyyy'), a year + month ('yyyy-dd'), a date, or a datetime."
        )

    if type_of_date in [_TypeOfDateString.DATETIME, _TypeOfDateString.DAY]:
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

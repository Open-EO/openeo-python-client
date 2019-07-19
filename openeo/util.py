"""
Various utilities and helpers.
"""
import re
from datetime import datetime, date
from typing import Any, Union, Tuple

_rfc3339_date_format = re.compile(r'\d{4}-\d{2}-\d{2}')


def date_to_rfc3339(d: Any) -> str:
    """
    Convert date-like object to a RFC 3339 formatted date string
    """
    if isinstance(d, str):
        if _rfc3339_date_format.match(d):
            return d
    elif isinstance(d, datetime):
        assert d.tzinfo is None, "timezone handling not supported (TODO)"
        return d.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif isinstance(d, date):
        return d.strftime('%Y-%m-%d')
    raise NotImplementedError("TODO")


def first_not_none(*args):
    """Return first item from given arguments that is not None."""
    for item in args:
        if item is not None:
            return item
    raise ValueError("No not-None values given.")


def get_temporal_extent(*args,
                        start_date: Union[str, datetime, date] = None, end_date: Union[str, datetime, date] = None,
                        extent: Union[list, tuple] = None,
                        convertor=date_to_rfc3339
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
            raise ValueError('Unable to handle {a!r} as a date range'.format(a=args))
    elif extent:
        assert start_date is None and end_date is None
        start_date, end_date = extent
    return convertor(start_date) if start_date else None, convertor(end_date) if end_date else None

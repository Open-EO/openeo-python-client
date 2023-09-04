"""
Various utilities and helpers.
"""

# TODO #465 split this kitchen-sink in thematic submodules

from __future__ import annotations

import datetime as dt
import functools
import json
import logging
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable, Optional, Tuple, Union
from urllib.parse import urljoin

import requests
import shapely.geometry.base
from deprecated import deprecated

try:
    # pyproj is an optional dependency
    import pyproj
except ImportError:
    pyproj = None


logger = logging.getLogger(__name__)


class Rfc3339:
    """
    Formatter for dates according to RFC-3339.

    Parses date(time)-like input and formats according to RFC-3339. Some examples:

        >>> rfc3339.date("2020:03:17")
        "2020-03-17"
        >>> rfc3339.date(2020, 3, 17)
        "2020-03-17"
        >>> rfc3339.datetime("2020/03/17/12/34/56")
        "2020-03-17T12:34:56Z"
        >>> rfc3339.datetime([2020, 3, 17, 12, 34, 56])
        "2020-03-17T12:34:56Z"
        >>> rfc3339.datetime(2020, 3, 17)
        "2020-03-17T00:00:00Z"
        >>> rfc3339.datetime(datetime(2020, 3, 17, 12, 34, 56))
        "2020-03-17T12:34:56Z"

    Or just normalize (automatically preserve date/datetime resolution):

        >>> rfc3339.normalize("2020/03/17")
        "2020-03-17"
        >>> rfc3339.normalize("2020-03-17-12-34-56")
        "2020-03-17T12:34:56Z"

    Also see https://tools.ietf.org/html/rfc3339#section-5.6
    """
    # TODO: currently we hard code timezone 'Z' for simplicity. Add real time zone support?
    _FMT_DATE = '%Y-%m-%d'
    _FMT_TIME = '%H:%M:%SZ'
    _FMT_DATETIME = _FMT_DATE + "T" + _FMT_TIME

    _regex_datetime = re.compile(r"""
        ^(?P<Y>\d{4})[:/_-](?P<m>\d{2})[:/_-](?P<d>\d{2})[T :/_-]?
        (?:(?P<H>\d{2})[:/_-](?P<M>\d{2})(?:[:/_-](?P<S>\d{2}))?)?""", re.VERBOSE)

    def __init__(self, propagate_none: bool = False):
        self._propagate_none = propagate_none

    def datetime(self, x: Any, *args) -> Union[str, None]:
        """
        Format given date(time)-like object as RFC-3339 datetime string.
        """
        if args:
            return self.datetime((x,) + args)
        elif isinstance(x, dt.datetime):
            return self._format_datetime(x)
        elif isinstance(x, dt.date):
            return self._format_datetime(dt.datetime.combine(x, dt.time()))
        elif isinstance(x, str):
            return self._format_datetime(dt.datetime(*self._parse_datetime(x)))
        elif isinstance(x, (tuple, list)):
            return self._format_datetime(dt.datetime(*(int(v) for v in x)))
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    def date(self, x: Any, *args) -> Union[str, None]:
        """
        Format given date-like object as RFC-3339 date string.
        """
        if args:
            return self.date((x,) + args)
        elif isinstance(x, (dt.date, dt.datetime)):
            return self._format_date(x)
        elif isinstance(x, str):
            return self._format_date(dt.datetime(*self._parse_datetime(x)))
        elif isinstance(x, (tuple, list)):
            return self._format_date(dt.datetime(*(int(v) for v in x)))
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    def normalize(self, x: Any, *args) -> Union[str, None]:
        """
        Format given date(time)-like object as RFC-3339 date or date-time string depending on given resolution

            >>> rfc3339.normalize("2020/03/17")
            "2020-03-17"
            >>> rfc3339.normalize("2020/03/17/12/34/56")
            "2020-03-17T12:34:56Z"
        """
        if args:
            return self.normalize((x,) + args)
        elif isinstance(x, dt.datetime):
            return self.datetime(x)
        elif isinstance(x, dt.date):
            return self.date(x)
        elif isinstance(x, str):
            x = self._parse_datetime(x)
            return self.date(x) if len(x) <= 3 else self.datetime(x)
        elif isinstance(x, (tuple, list)):
            return self.date(x) if len(x) <= 3 else self.datetime(x)
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    def parse_date(self, x: Union[str, None]) -> Union[dt.date, None]:
        """Parse given string as RFC3339 date."""
        if isinstance(x, str):
            return dt.datetime.strptime(x, '%Y-%m-%d').date()
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    def parse_datetime(
        self, x: Union[str, None], with_timezone: bool = False
    ) -> Union[dt.datetime, None]:
        """Parse given string as RFC3339 date-time."""
        if isinstance(x, str):
            # TODO: Also support parsing other timezones than UTC (Z)
            if re.search(r":\d+\.\d+", x):
                res = dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                res = dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ")
            if with_timezone:
                res = res.replace(tzinfo=dt.timezone.utc)
            return res
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    def parse_date_or_datetime(
        self, x: Union[str, None], with_timezone: bool = False
    ) -> Union[dt.date, dt.datetime, None]:
        """Parse given string as RFC3339 date or date-time."""
        if isinstance(x, str):
            if len(x) > 10:
                return self.parse_datetime(x, with_timezone=with_timezone)
            else:
                return self.parse_date(x)
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    @classmethod
    def _format_datetime(cls, d: dt.datetime) -> str:
        """Format given datetime as RFC-3339 date-time string."""
        if d.tzinfo not in {None, dt.timezone.utc}:
            # TODO: add support for non-UTC timezones?
            raise ValueError(f"No support for non-UTC timezone {d.tzinfo}")
        return d.strftime(cls._FMT_DATETIME)

    @classmethod
    def _format_date(cls, d: dt.date) -> str:
        """Format given datetime as RFC-3339 date-time string."""
        return d.strftime(cls._FMT_DATE)

    @classmethod
    def _parse_datetime(cls, s: str) -> Tuple[int]:
        """Try to parse string to a date(time) tuple"""
        try:
            return tuple(int(v) for v in cls._regex_datetime.match(s).groups() if v is not None)
        except Exception:
            raise ValueError("Can not parse as date: {s}".format(s=s))

    def today(self) -> str:
        """Today (date) in RFC3339 format"""
        return self.date(dt.date.today())

    def utcnow(self) -> str:
        """Current UTC datetime in RFC3339 format."""
        # Current time in UTC timezone (instead of naive `datetime.datetime.utcnow()`, per `datetime` documentation)
        now = dt.datetime.now(tz=dt.timezone.utc)
        return self.datetime(now)


# Default RFC3339 date-time formatter
rfc3339 = Rfc3339()


@deprecated("Use `rfc3339.normalize`, `rfc3339.date` or `rfc3339.datetime` instead")
def date_to_rfc3339(d: Any) -> str:
    """
    Convert date-like object to a RFC 3339 formatted date string

    see https://tools.ietf.org/html/rfc3339#section-5.6
    """
    return rfc3339.normalize(d)


def dict_no_none(*args, **kwargs) -> dict:
    """
    Helper to build a dict containing given key-value pairs where the value is not None.
    """
    return {
        k: v
        for k, v in dict(*args, **kwargs).items()
        if v is not None
    }


def first_not_none(*args):
    """Return first item from given arguments that is not None."""
    for item in args:
        if item is not None:
            return item
    raise ValueError("No not-None values given.")


def ensure_dir(path: Union[str, Path]) -> Path:
    """Create directory if it doesn't exist."""
    path = Path(path)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    assert path.is_dir()
    return path


def ensure_list(x):
    """Convert given data structure to a list."""
    try:
        return list(x)
    except TypeError:
        return [x]


def get_temporal_extent(*args,
                        start_date: Union[str, dt.datetime, dt.date] = None,
                        end_date: Union[str, dt.datetime, dt.date] = None,
                        extent: Union[list, tuple] = None,
                        convertor=rfc3339.normalize
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
    if start_date and not end_date and isinstance(start_date, str):
        start_date, end_date = string_to_temporal_extent(start_date)
    return convertor(start_date) if start_date else None, convertor(end_date) if end_date else None


def string_to_temporal_extent(
    start_date: Union[str, dt.datetime, dt.date]
) -> Tuple[Union[dt.date, dt.datetime, str], Union[dt.date, dt.datetime, None]]:
    """Convert a string into a date range when it is an abbreviation for an entire year or month.

    The result is a 2-tuple ``(start, end)`` that represents the period as a
    half-open interval, where the end date is not included in the period.

    The intent of this function is to only convert values into a periods
    when they are clearly abbreviations, and in all other cases leave the original
    start_date as it was, because there can be too many complications otherwise.

    The reason being that calling functions, e.g. ``get_temporal_extent``,
    can allow you to specifying both a start date **and** end date, but either date
    can be ``None``. What such an open-ended interval means depends very much on
    what the calling function/method is meant to do, so the caller should really
    handle that themselves.

    When we don't convert, the return value is the tuple ``(start_date, None)``
    using the original parameter value start_date, unprocessed.

    :param start_date:

        - Typically a string that represents either a year, a year + month, a day,
            or a datetime, and it always indicates the *beginning* of that period.
        - Other data types allowed are a ``datetime.date`` and ``datetime.datetime``,
            and in that case we return the tuple ``(start_date, None)`` where
            ``start_date`` is our original input parameter ``start_date`` as-is.
            Similarly, strings that represent a date or datetime are not processed
            any further and the return value is also ``(start_date, None)``.
        - Any other type raises a TypeError.

        - Allowed string formats are:
            - For year: "yyyy"
            - For year + month: "yyyy-mm"
                Some other separators than "-" technically work but they are discouraged.
            - For date and datetime you must follow the RFC 3339 format. See also: class ``Rfc3339``

    :return:
        The result is a 2-tuple of the form ``(start, end)`` that represents
        the period as a half-open interval, where the end date is not included,
        i.e. end is the first day that is no longer part of the time slot.

        When start_date was indeed an abbreviation and thus was converted to
        a period, then the element types will be ``(datetime.date, datetime.date)``

        If no conversion happened we return the original start_date wrapped in a
        2-tuple:  ``(start_date, None)`` so the type is the same as the input's type.

    :raises TypeError:
        when start_date is neither of the following types:
        str, datetime.date, datetime.datetime

    :raises ValueError:
        when start_date was a string but not recognized as either a year,
        a month, a date, or datetime.

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
    supported_types = (str, dt.date, dt.datetime)
    if not isinstance(start_date, supported_types):
        raise TypeError(
            "Value of start_date must be one of the following types:"
            + "str, datetime.date, datetime.datetime"
            + f"but it is {type(start_date)}, value={start_date}"
        )

    # Skip it if the string represents a day or if it is not even a string
    # If it is a day, we want to let the upstream function handle that case
    # because a day could be either a start date or an end date.
    if not isinstance(start_date, str):
        return start_date, None

    # Using a separate and stricter regular expressions to detect day, month,
    # or year. Having a regex that only matches one type of period makes it
    # easier to check it is effectively only a year, or only a month,
    # but not a day. Datetime strings are more complex so we use rfc3339 to
    # check whether or not it represents a datetime.
    regex_day = re.compile(r"^(\d{4})[:/_-](\d{2})[:/_-](\d{2})$")
    regex_month = re.compile(r"^(\d{4})[:/_-](\d{2})$")
    regex_year = re.compile(r"^\d{4}$")

    try:
        rfc3339.parse_datetime(start_date)
        is_date_time = True
    except ValueError as exc:
        is_date_time = False

    match_day = regex_day.match(start_date)
    match_month = regex_month.match(start_date)
    match_year = regex_year.match(start_date)

    if is_date_time or match_day:
        return start_date, None

    if not (match_year or match_month):
        raise ValueError(
            f"The value of start_date='{start_date}' does not represent any of: "
            + "a year ('yyyy'), a year + month ('yyyy-dd'), a date, or a datetime."
        )

    if match_month:
        year_start = int(match_month.group(1))
        month_start = int(match_month.group(2))
        if month_start == 12:
            year_end = year_start + 1
            month_end = 1
        else:
            month_end = month_start + 1
            year_end = year_start
    else:
        year_start = int(start_date)
        year_end = year_start + 1
        month_start = 1
        month_end = 1

    date_start = dt.date(year_start, month_start, 1)
    date_end = dt.date(year_end, month_end, 1)

    return date_start, date_end


class ContextTimer:
    """
    Context manager to measure the "wall clock" time (in seconds) inside/for a block of code.

    Usage example:

        with ContextTimer() as timer:
            # Inside code block: currently elapsed time
            print(timer.elapsed())

        # Outside code block: elapsed time when block ended
        print(timer.elapsed())

    """
    __slots__ = ["start", "end"]

    # Function that returns current time in seconds (overridable for unit tests)
    _clock = time.time

    def __init__(self):
        self.start = None
        self.end = None

    def elapsed(self) -> float:
        """Elapsed time (in seconds) inside or at the end of wrapped context."""
        if self.start is None:
            raise RuntimeError("Timer not started.")
        if self.end is not None:
            # Elapsed time when exiting context.
            return self.end - self.start
        else:
            # Currently elapsed inside context.
            return self._clock() - self.start

    def __enter__(self) -> ContextTimer:
        self.start = self._clock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = self._clock()


class TimingLogger:
    """
    Context manager for quick and easy logging of start time, end time and elapsed time of some block of code

    Usage example:

    >>> with TimingLogger("Doing batch job"):
    ...     do_batch_job()

    At start of the code block the current time will be logged
    and at end of the code block the end time and elapsed time will be logged.

    Can also be used as a function/method decorator, for example:

    >>> @TimingLogger("Calculation going on")
    ... def add(x, y):
    ...     return x + y
    """

    # Function that returns current datetime (overridable for unit tests)
    _now = dt.datetime.now

    def __init__(self, title: str = "Timing", logger: Union[logging.Logger, str, Callable] = logger):
        """
        :param title: the title to use in the logging
        :param logger: how the timing should be logged.
            Can be specified as a logging.Logger object (in which case the INFO log level will be used),
            as a string (name of the logging.Logger object to construct),
            or as callable (e.g. to use the `print` function, or the `.debug` method of an existing logger)
        """
        self.title = title
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        if isinstance(logger, (logging.Logger, logging.LoggerAdapter)):
            self._log = logger.info
        elif callable(logger):
            self._log = logger
        else:
            raise ValueError("Invalid logger {l!r}".format(l=logger))

        self.start_time = self.end_time = self.elapsed = None

    def __enter__(self):
        self.start_time = self._now()
        self._log("{t}: start {s}".format(t=self.title, s=self.start_time))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = self._now()
        self.elapsed = self.end_time - self.start_time
        self._log("{t}: {s} {e}, elapsed {d}".format(
            t=self.title,
            s="fail" if exc_type else "end",
            e=self.end_time, d=self.elapsed
        ))

    def __call__(self, f: Callable):
        """
        Use TimingLogger as function/method decorator
        """

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            with self:
                return f(*args, **kwargs)

        return wrapper


class DeepKeyError(LookupError):
    def __init__(self, key, keys):
        super(DeepKeyError, self).__init__("{k!r} (from deep key {s!r})".format(k=key, s=keys))


# Sentinel object for `default` argument of `deep_get`
_deep_get_default_undefined = object()


def deep_get(data: dict, *keys, default=_deep_get_default_undefined):
    """
    Get value deeply from nested dictionaries/lists/tuples

    :param data: nested data structure of dicts, lists, tuples
    :param keys: sequence of keys/indexes to traverse
    :param default: default value when a key is missing.
        By default a DeepKeyError will be raised.
    :return:
    """
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        elif isinstance(data, (list, tuple)) and isinstance(key, int) and 0 <= key < len(data):
            data = data[key]
        else:
            if default is _deep_get_default_undefined:
                raise DeepKeyError(key, keys)
            else:
                return default
    return data


def deep_set(data: dict, *keys, value):
    """
    Set a value deeply in nested dictionary

    :param data: nested data structure of dicts, lists, tuples
    :param keys: sequence of keys/indexes to traverse
    :param value: value to set
    """
    if len(keys) == 1:
        data[keys[0]] = value
    elif len(keys) > 1:
        if isinstance(data, dict):
            deep_set(data.setdefault(keys[0], OrderedDict()), *keys[1:], value=value)
        elif isinstance(data, (list, tuple)):
            deep_set(data[keys[0]], *keys[1:], value=value)
        else:
            ValueError(data)
    else:
        raise ValueError("No keys given")


def guess_format(filename: Union[str, Path]) -> str:
    """
    Guess the output format from a given filename and return the corrected format.
    Any names not in the dict get passed through.
    """
    extension = str(filename).rsplit(".", 1)[-1].lower()

    format_map = {
        "gtiff": "GTiff", "geotiff": "GTiff", "geotif": "GTiff", "tiff": "GTiff", "tif": "GTiff",
        "nc": "netCDF", "netcdf": "netCDF",
        "geojson": "GeoJSON",
    }

    return format_map.get(extension, extension.upper())


def load_json(path: Union[Path, str]) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def load_json_resource(src: Union[str, Path]) -> dict:
    """
    Helper to load some kind of JSON resource

    :param src: a JSON resource: a raw JSON string,
        a path to (local) JSON file, or a URL to a remote JSON resource
    :return: data structured parsed from JSON
    """
    if isinstance(src, str) and src.strip().startswith("{"):
        # Assume source is a raw JSON string
        return json.loads(src)
    elif isinstance(src, str) and re.match(r"^https?://", src, flags=re.I):
        # URL to remote JSON resource
        return requests.get(src).json()
    elif isinstance(src, Path) or (isinstance(src, str) and src.endswith(".json")):
        # Assume source is a local JSON file path
        return load_json(src)
    raise ValueError(src)


class LazyLoadCache:
    """Simple cache that allows to (lazy) load on cache miss."""

    def __init__(self):
        self._cache = {}

    def get(self, key: Union[str, tuple], load: Callable[[], Any]):
        if key not in self._cache:
            self._cache[key] = load()
        return self._cache[key]


def str_truncate(text: str, width: int = 64, ellipsis: str = "...") -> str:
    """Shorten a string (with an ellipsis) if it is longer than certain length."""
    width = max(0, int(width))
    if len(text) <= width:
        return text
    if len(ellipsis) > width:
        ellipsis = ellipsis[:width]
    return text[:max(0, (width - len(ellipsis)))] + ellipsis


def repr_truncate(obj: Any, width: int = 64, ellipsis: str = "...") -> str:
    """Do `repr` rendering of an object, but truncate string if it is too long ."""
    if isinstance(obj, str) and width > len(ellipsis) + 2:
        # Special case: put ellipsis inside quotes
        return repr(str_truncate(text=obj, width=width - 2, ellipsis=ellipsis))
    else:
        # General case: just put ellipsis at end
        return str_truncate(text=repr(obj), width=width, ellipsis=ellipsis)


def in_interactive_mode() -> bool:
    """Detect if we are running in interactive mode (Jupyter/IPython/repl)"""
    # Based on https://stackoverflow.com/a/64523765
    return hasattr(sys, "ps1")


class InvalidBBoxException(ValueError):
    pass


class BBoxDict(dict):
    """
    Dictionary based helper to easily create/work with bounding box dictionaries
    (having keys "west", "south", "east", "north", and optionally "crs").

    crs:
        value that encodes a coordinate reference system, typically just an int (EPSG code) or string (authority string).

        Integers always refer to the corresponding EPSG code.
        A string that contains only an integer is interpreted as that same integer EPSG code.

        You can also specify EPSG codes as an authority string, such as "EPSG:4326".
        For example, the following string and int values all specify to the same CRS:
        ``"EPSG:4326"``, ``"4326"``, and the integer ``4326``.

        .. seealso:: openEO Python Client documentation on openeo.util.normalize_crs  :py:func:`openeo.util.normalize_crs` for the most up to date information.

    .. versionadded:: 0.10.1
    """

    def __init__(self, *, west: float, south: float, east: float, north: float, crs: Optional[Union[str, int]] = None):
        super().__init__(west=west, south=south, east=east, north=north)
        if crs is not None:
            self.update(crs=normalize_crs(crs))

    # TODO: provide west, south, east, north, crs as @properties? Read-only or read-write?

    @classmethod
    def from_any(cls, x: Any, *, crs: Optional[str] = None) -> BBoxDict:
        if isinstance(x, dict):
            if crs and "crs" in x and crs != x["crs"]:
                raise InvalidBBoxException(f"Two CRS values specified: {crs} and {x['crs']}")
            return cls.from_dict({"crs": crs, **x})
        elif isinstance(x, (list, tuple)):
            return cls.from_sequence(x, crs=crs)
        elif isinstance(x, shapely.geometry.base.BaseGeometry):
            return cls.from_sequence(x.bounds, crs=crs)
        # TODO: support other input? E.g.: WKT string, GeoJson-style dictionary (Polygon, FeatureCollection, ...)
        else:
            raise InvalidBBoxException(f"Can not construct BBoxDict from {x!r}")

    @classmethod
    def from_dict(cls, data: dict) -> BBoxDict:
        """Build from dictionary with at least keys "west", "south", "east", and "north"."""
        expected_fields = {"west", "south", "east", "north"}
        # TODO: also support upper case fields?
        # TODO: optional support for parameterized bbox fields?
        missing = expected_fields.difference(data.keys())
        if missing:
            raise InvalidBBoxException(f"Missing bbox fields {sorted(missing)}")
        invalid = {k: data[k] for k in expected_fields if not isinstance(data[k], (int, float))}
        if invalid:
            raise InvalidBBoxException(f"Non-numerical bbox fields {invalid}.")
        return cls(west=data["west"], south=data["south"], east=data["east"], north=data["north"], crs=data.get("crs"))

    @classmethod
    def from_sequence(cls, seq: Union[list, tuple], crs: Optional[str] = None) -> BBoxDict:
        """Build from sequence of 4 bounds (west, south, east and north)."""
        if len(seq) != 4:
            raise InvalidBBoxException(f"Expected sequence with 4 items, but got {len(seq)}.")
        return cls(west=seq[0], south=seq[1], east=seq[2], north=seq[3], crs=crs)


def to_bbox_dict(x: Any, *, crs: Optional[Union[str, int]] = None) -> BBoxDict:
    """
    Convert given data or object to a bounding box dictionary
    (having keys "west", "south", "east", "north", and optionally "crs").

    Supports various input types/formats:

    - list/tuple (assumed to be in west-south-east-north order)

        >>> to_bbox_dict([3, 50, 4, 51])
        {'west': 3, 'south': 50, 'east': 4, 'north': 51}

    - dictionary (unnecessary items will be stripped)

        >>> to_bbox_dict({
        ...     "color": "red", "shape": "triangle",
        ...     "west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326",
        ... })
        {'west': 1, 'south': 2, 'east': 3, 'north': 4, 'crs': 'EPSG:4326'}

    - a shapely geometry

    .. versionadded:: 0.10.1

    :param x: input data that describes west-south-east-north bounds in some way, e.g. as a dictionary,
        a list, a tuple, ashapely geometry, ...
    :param crs: (optional) CRS field
    :return: dictionary (subclass) with keys "west", "south", "east", "north", and optionally "crs".
    """
    return BBoxDict.from_any(x=x, crs=crs)


def url_join(root_url: str, path: str):
    """Join a base url and sub path properly."""
    return urljoin(root_url.rstrip("/") + "/", path.lstrip("/"))


def clip(x: float, min: float, max: float) -> float:
    """Clip given value between minimum and maximum value"""
    return min if x < min else (x if x < max else max)


class SimpleProgressBar:
    """Simple ASCII-based progress bar helper."""

    __slots__ = ["width", "bar", "fill", "left", "right"]

    def __init__(self, width: int = 40, *, bar: str = "#", fill: str = "-", left: str = "[", right: str = "]"):
        self.width = int(width)
        self.bar = bar[0]
        self.fill = fill[0]
        self.left = left
        self.right = right

    def get(self, fraction: float) -> str:
        width = self.width - len(self.left) - len(self.right)
        bar = self.bar * int(round(width * clip(fraction, min=0, max=1)))
        return f"{self.left}{bar:{self.fill}<{width}s}{self.right}"


def normalize_crs(crs: Any, *, use_pyproj: bool = True) -> Union[None, int, str]:
    """
    Normalize given data structure (typically just an int or string)
    that encodes a CRS (Coordinate Reference System) to an EPSG (int) code or WKT2 CRS string.

    Behavior and data structure support depends on the availability of the ``pyproj`` library:

    -   If the ``pyproj`` library is available: use that to do parsing and conversion.
        This means that everything supported by `pyproj.CRS.from_user_input <https://pyproj4.github.io/pyproj/dev/api/crs/crs.html#pyproj.crs.CRS.from_user_input>`_ is allowed.
        See the ``pyproj`` docs for more details.
    -   Otherwise, some best effort validation is done:
        EPSG looking int/str values will be parsed as such, other strings will be assumed to be WKT2 already.
        Other data structures will not be accepted.

        Integers always refer to the corresponding EPSG code.
        A string that contains only an integer is interpreted as that same integer EPSG code.

        You can also specify EPSG codes as an authority string, such as "EPSG:4326".
        For example, the following string and int values all specify to the same CRS:
        ``"EPSG:4326"``, ``"4326"``, and the integer ``4326``.

    :param crs: value that encodes a coordinate reference system, typically just an int (EPSG code) or string (authority string).
        If the ``pyproj`` library is available, everything supported by it is allowed.

    :param use_pyproj: whether ``pyproj`` should be leveraged at all
        (mainly useful for testing the "no pyproj available" code path)

    :return: EPSG code as int, or WKT2 string. Or None if input was empty.

    :raises ValueError:
        When the given CRS data can not be parsed/converted/normalized.

    """
    if crs in (None, "", {}):
        return None

    if pyproj and use_pyproj:
        try:
            # (if available:) let pyproj do the validation/parsing
            crs_obj = pyproj.CRS.from_user_input(crs)
            # Convert back to EPSG int or WKT2 string
            crs = crs_obj.to_epsg() or crs_obj.to_wkt()
        except pyproj.ProjError as e:
            raise ValueError(f"Failed to normalize CRS data with pyproj: {crs!r}") from e
    else:
        # Best effort simple validation/normalization
        if isinstance(crs, int) and crs > 0:
            # Assume int is already valid EPSG code
            pass
        elif isinstance(crs, str):
            # Parse as EPSG int code if it looks like that,
            # otherwise: leave it as-is, assuming it is a valid WKT2 CRS string
            if re.match(r"^(epsg:)?\d+$", crs.strip(), flags=re.IGNORECASE):
                crs = int(crs.split(":")[-1])
            elif "GEOGCRS[" in crs:
                # Very simple WKT2 CRS detection heuristic
                logger.warning(f"Assuming this is a valid WK2 CRS string: {repr_truncate(crs)}")
            else:
                raise ValueError(f"Can not normalize CRS string {repr_truncate(crs)}")
        else:
            raise ValueError(f"Can not normalize CRS data {type(crs)}")

    return crs

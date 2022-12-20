"""
Various utilities and helpers.
"""
import datetime as dt
import functools
import json
import logging
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Union, Tuple, Callable, Optional
from urllib.parse import urljoin

import requests
import shapely.geometry.base
from deprecated import deprecated

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

    def parse_datetime(self, x: Union[str, None]) -> Union[dt.datetime, None]:
        """Parse given string as RFC3339 date-time."""
        if isinstance(x, str):
            return dt.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ')
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    def parse_date_or_datetime(self, x: Union[str, None]) -> Union[dt.date, dt.datetime, None]:
        """Parse given string as RFC3339 date or date-time."""
        if isinstance(x, str):
            if len(x) > 10:
                return self.parse_datetime(x)
            else:
                return self.parse_date(x)
        elif x is None and self._propagate_none:
            return None
        raise ValueError(x)

    @classmethod
    def _format_datetime(cls, d: dt.datetime) -> str:
        """Format given datetime as RFC-3339 date-time string."""
        assert d.tzinfo is None, "timezone handling not supported (TODO)"
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
    return convertor(start_date) if start_date else None, convertor(end_date) if end_date else None


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

    def __enter__(self) -> 'ContextTimer':
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
        if isinstance(logger, logging.Logger):
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


def guess_format(filename: Union[str, Path]):
    """
    Guess the output format from a given filename and return the corrected format.
    Any names not in the dict get passed through.
    """
    extension = str(filename).rsplit(".", 1)[-1].lower()

    format_map = {
        "gtiff": "GTiff", "geotiff": "GTiff", "geotif": "GTiff", "tiff": "GTiff", "tif": "GTiff",
        "nc": "netCDF", "netcdf": "netCDF",
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


class BBoxDict(dict):
    """
    Dictionary based helper to easily create/work with bounding box dictionaries
    (having keys "west", "south", "east", "north", and optionally "crs").

    .. versionadded:: 0.10.1
    """

    def __init__(self, *, west: float, south: float, east: float, north: float, crs: Optional[str] = None):
        super().__init__(west=west, south=south, east=east, north=north)
        if crs is not None:
            self.update(crs=crs)

    # TODO: provide west, south, east, north, crs as @properties? Read-only or read-write?

    @classmethod
    def from_any(cls, x: Any, *, crs: Optional[str] = None) -> 'BBoxDict':
        if isinstance(x, dict):
            return cls.from_dict({"crs": crs, **x})
        elif isinstance(x, (list, tuple)):
            return cls.from_sequence(x, crs=crs)
        elif isinstance(x, shapely.geometry.base.BaseGeometry):
            return cls.from_sequence(x.bounds, crs=crs)
        # TODO: support other input? E.g.: WKT string, GeoJson-style dictionary (Polygon, FeatureCollection, ...)
        else:
            raise ValueError(f"Can not construct BBoxDict from {x!r}")

    @classmethod
    def from_dict(cls, data: dict) -> 'BBoxDict':
        """Build from dictionary with at least keys "west", "south", "east", and "north"."""
        expected_fields = {"west", "south", "east", "north"}
        # TODO: also support converting support case fields?
        if not all(k in data for k in expected_fields):
            raise ValueError(
                f"Expecting fields {expected_fields}, but only found {expected_fields.intersection(data.keys())}."
            )
        return cls(
            west=data["west"], south=data["south"], east=data["east"], north=data["north"],
            crs=data.get("crs")
        )

    @classmethod
    def from_sequence(cls, seq: Union[list, tuple], crs: Optional[str] = None) -> 'BBoxDict':
        """Build from sequence of 4 bounds (west, south, east and north)."""
        if len(seq) != 4:
            raise ValueError(f"Expected sequence with 4 items, but got {len(seq)}.")
        return cls(west=seq[0], south=seq[1], east=seq[2], north=seq[3], crs=crs)


def to_bbox_dict(x: Any, *, crs: Optional[str] = None) -> BBoxDict:
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

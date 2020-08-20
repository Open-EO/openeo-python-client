"""
Various utilities and helpers.
"""
import datetime as dt
import functools
import json
import logging
import os
import platform
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any, Union, Tuple, Callable

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
        Format given date(time)-like object as RFC-333 date or date-time string depending on given resolution

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

    def parse_datetime(self, x: Union[str, None]) -> dt.datetime:
        if isinstance(x, str):
            return dt.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ')
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


def dict_no_none(**kwargs):
    """
    Helper to build a dict containing given key-value pairs where the value is not None.
    """
    return {
        k: v
        for k, v in kwargs.items()
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
                return f(*args, *kwargs)

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


def load_json(path: Union[Path, str]) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


DEFAULT_APP_NAME = "openeo-python-client"


def _get_user_dir(
        app_name=DEFAULT_APP_NAME,
        xdg_env_var='XDG_CONFIG_HOME',
        win_env_var='APPDATA',
        fallback='~/.config',
        win_fallback='~\\AppData\\Roaming',
        macos_fallback='~/Library/Preferences',
        auto_create=True,
) -> Path:
    """
    Get platform specific config/data/cache folder
    """
    # Platform specific root locations (from highest priority to lowest)
    env = os.environ
    if platform.system() == 'Windows':
        roots = [env.get(win_env_var), win_fallback, fallback]
    elif platform.system() == 'Darwin':
        roots = [env.get(xdg_env_var), macos_fallback, fallback]
    else:
        # Assume unix
        roots = [env.get(xdg_env_var), fallback]

    # Filter out None's, expand user prefix and append app name
    dirs = [Path(r).expanduser() / app_name for r in roots if r]
    # Prepend with OPENEO_CONFIG_HOME if set.
    if env.get("OPENEO_CONFIG_HOME"):
        dirs.insert(0, Path(env.get("OPENEO_CONFIG_HOME")))

    # Use highest prio dir that already exists.
    for p in dirs:
        if p.exists() and p.is_dir():
            return p

    # No existing dir: create highest prio one (if possible)
    if auto_create:
        for p in dirs:
            try:
                p.mkdir(parents=True)
                logger.info("Created user dir for {a!r}: {p}".format(a=app_name, p=p))
                return p
            except OSError:
                pass

    raise Exception("Failed to find user dir for {a!r}. Tried: {p!r}".format(a=app_name, p=dirs))


def get_user_config_dir(app_name=DEFAULT_APP_NAME, auto_create=True) -> Path:
    """
    Get platform specific config folder
    """
    return _get_user_dir(
        app_name=app_name,
        xdg_env_var='XDG_CONFIG_HOME', win_env_var='APPDATA',
        fallback='~/.config', win_fallback='~\\AppData\\Roaming', macos_fallback='~/Library/Preferences',
        auto_create=auto_create
    )


def get_user_data_dir(app_name=DEFAULT_APP_NAME, auto_create=True) -> Path:
    """
    Get platform specific data folder
    """
    return _get_user_dir(
        app_name=app_name,
        xdg_env_var='XDG_DATA_HOME', win_env_var='APPDATA',
        fallback='~/.local/share', win_fallback='~\\AppData\\Roaming', macos_fallback='~/Library',
        auto_create=auto_create
    )

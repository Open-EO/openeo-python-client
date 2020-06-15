"""
Various utilities and helpers.
"""
import functools
import json
import logging
import os
import platform
import re
from datetime import datetime, date
from pathlib import Path
from typing import Any, Union, Tuple, Callable

_rfc3339_date_format = re.compile(r'\d{4}-\d{2}-\d{2}')

logger = logging.getLogger(__name__)


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
    _now = datetime.now

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
    Get "deep" value from nested dictionaries/lists/tuples

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


def load_json(path: Union[Path, str]) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def _get_user_dir(
        app_name="openeo-python-client",
        xdg_env_var='XDG_CONFIG_HOME',
        win_env_var='APPDATA',
        fallback='~/.config',
        win_fallback='~\\AppData\\Roaming',
        macos_fallback='~/Library/Preferences',
        auto_create=True
) -> Path:
    """
    Get platform specific config/data/cache folder
    """
    # Platform specific root locations (from highest priority to lowest)
    if platform.system() == 'Windows':
        roots = [os.environ.get(win_env_var), win_fallback, fallback]
    elif platform.system() == 'Darwin':
        roots = [os.environ.get(xdg_env_var), macos_fallback, fallback]
    else:
        # Assume unix
        roots = [os.environ.get(xdg_env_var), fallback]

    # Filter out None's, expand user prefix and append app name
    dirs = [Path(r).expanduser() / app_name for r in roots if r]

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


def get_user_config_dir(app_name="openeo-python-client", auto_create=True) -> Path:
    """
    Get platform specific config folder
    """
    return _get_user_dir(
        app_name=app_name,
        xdg_env_var='XDG_CONFIG_HOME', win_env_var='APPDATA',
        fallback='~/.config', win_fallback='~\\AppData\\Roaming', macos_fallback='~/Library/Preferences',
        auto_create=auto_create
    )


def get_user_data_dir(app_name="openeo-python-client", auto_create=True) -> Path:
    """
    Get platform specific data folder
    """
    return _get_user_dir(
        app_name=app_name,
        xdg_env_var='XDG_DATA_HOME', win_env_var='APPDATA',
        fallback='~/.local/share', win_fallback='~\\AppData\\Roaming', macos_fallback='~/Library',
        auto_create=auto_create
    )

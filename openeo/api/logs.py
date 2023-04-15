import logging
from typing import Optional, Union


class LogEntry(dict):
    """
    Log message and info for jobs and services

    Fields:
        - ``id``: Unique ID for the log, string, REQUIRED
        - ``code``: Error code, string, optional
        - ``level``: Severity level, string (error, warning, info or debug), REQUIRED
        - ``message``: Error message, string, REQUIRED
        - ``time``: Date and time of the error event as RFC3339 date-time, string, available since API 1.1.0
        - ``path``: A "stack trace" for the process, array of dicts
        - ``links``: Related links, array of dicts
        - ``usage``: Usage metrics available as property 'usage', dict, available since API 1.1.0
          May contain the following metrics: cpu, memory, duration, network, disk, storage and other custom ones
          Each of the metrics is also a dict with the following parts: value (numeric) and unit (string)
        - ``data``: Arbitrary data the user wants to "log" for debugging purposes.
          Please note that this property may not exist as there's a difference
          between None and non-existing. None for example refers to no-data in
          many cases while the absence of the property means that the user did
          not provide any data for debugging.
    """

    _required = {"id", "level", "message"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Check required fields
        missing = self._required.difference(self.keys())
        if missing:
            raise ValueError("Missing required fields: {m}".format(m=sorted(missing)))

    @property
    def id(self):
        return self["id"]

    # Legacy alias
    log_id = id

    @property
    def message(self):
        return self["message"]

    @property
    def level(self):
        return self["level"]

    # TODO: add properties for "code", "time", "path", "links" and "data" with sensible defaults?


def normalize_log_level(
    log_level: Union[int, str, None], default: int = logging.DEBUG
) -> int:
    """
    Helper function to convert a openEO API log level (e.g. string "error")
    to the integer constants defined in Python's standard library ``logging`` module (e.g. ``logging.ERROR``).

    :param log_level: log level to normalize: a log level string in the style of
        the openEO API ("error", "warning", "info", or "debug"),
        an integer value (e.g. a ``logging`` constant), or ``None``.

    :param default: fallback log level to return on unknown log level strings or ``None`` input.

    :raises TypeError: when log_level is any other type than str, an int or None.
    :return: One of the following log level constants from the standard module ``logging``:
        ``logging.ERROR``, ``logging.WARNING``, ``logging.INFO``, or ``logging.DEBUG`` .
    """
    if isinstance(log_level, str):
        log_level = log_level.upper()
        if log_level in ["CRITICAL", "ERROR", "FATAL"]:
            return logging.ERROR
        elif log_level in ["WARNING", "WARN"]:
            return logging.WARNING
        elif log_level == "INFO":
            return logging.INFO
        elif log_level == "DEBUG":
            return logging.DEBUG
        else:
            return default
    elif isinstance(log_level, int):
        return log_level
    elif log_level is None:
        return default
    else:
        raise TypeError(
            f"Value for log_level is not an int or str: type={type(log_level)}, value={log_level!r}"
        )


def log_level_name(log_level: Union[int, str, None]) -> str:
    """
    Get the name of a normalized log level.
    This value conforms to log level names used in the openEO API.
    """
    return logging.getLevelName(normalize_log_level(log_level)).lower()

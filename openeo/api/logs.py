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


def normalize_log_level(log_level: Optional[Union[int, str]]) -> int:
    """Helper function to convert a log level to the integer constants defined in logging, e.g. logging.ERROR.

    Essentially log_level may be user input, or come from a method/function parameter filled in
    by a user of the Python client.

    :param log_level: the input to be converted.
    :raises TypeError: when log_level it is neither a str, an int or None.
    :return: one of the following log level constants from the standard module ``logging``:
        logging.ERROR, logging.WARNING, logging.INFO, or logging.DEBUG
    """
    if log_level is None:
        return logging.ERROR

    if isinstance(log_level, str):
        return string_to_log_level(log_level)

    # Now is should be an int
    if not isinstance(log_level, int):
        raise TypeError(
            f"Value for log_level is not an int or str: type={type(log_level)}, value={log_level!r}"
        )

    return log_level


def string_to_log_level(log_level: str) -> int:
    """Helper function: a simpler conversion of a log level, to use when you know log_level should **always** be a string.

    :param log_level: the input to be converted.
    :raises TypeError: when log_level it not a string.
    :return: one of the following log level constants from the standard module ``logging``:
        logging.ERROR, logging.WARNING, logging.INFO, or logging.DEBUG
    """

    if not isinstance(log_level, str):
        raise TypeError(
            f"Parameter 'log_level' must be type str, but it is type {type(log_level)}. Value: log_level={log_level!r}"
        )
    if log_level == "":
        return logging.ERROR

    log_level = log_level.upper()
    if log_level in ["CRITICAL", "ERROR"]:
        return logging.ERROR
    elif log_level == "WARNING":
        return logging.WARNING
    elif log_level == "INFO":
        return logging.INFO
    elif log_level == "DEBUG":
        return logging.DEBUG

    return logging.ERROR

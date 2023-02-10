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


def normalize_log_level(log_level: Optional[Union[int, str]] = logging.DEBUG) -> int:
    """Helper function to convert a log level to the integer constants defined in logging, e.g. ``logging.ERROR``.

    :param log_level: The input to be converted.

        The value  may be user input, or it can come from a method/function parameter
        filled in by the user of the Python client, so it is not necessarily valid.

        If no value is given or it is None, the empty string, or even any other 'falsy' value,
        then the default return value is ``logging.DEBUG``.

    :raises TypeError: when log_level is any other type than str, an int or None.
    :return: One of the following log level constants from the standard module ``logging``:
        ``logging.ERROR``, ``logging.WARNING``, ``logging.INFO``, or ``logging.DEBUG`` .
    """

    # None and the empty string could be passed explicitly (or other falsy values).
    # Or the value could come from a field that is None.
    if not log_level:
        return logging.DEBUG

    if isinstance(log_level, str):
        log_level = log_level.upper()
        if log_level in ["CRITICAL", "ERROR"]:
            return logging.ERROR
        elif log_level == "WARNING":
            return logging.WARNING
        elif log_level == "INFO":
            return logging.INFO
        elif log_level == "DEBUG":
            return logging.DEBUG

        # Still a string, but not a supported/standard log level.
        return logging.ERROR

    # Now it should be an int, otherwise the input is an unsupported type.
    if not isinstance(log_level, int):
        raise TypeError(
            f"Value for log_level is not an int or str: type={type(log_level)}, value={log_level!r}"
        )

    return log_level

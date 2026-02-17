"""
Debug utilities for UDFs
"""
import logging
import os
import sys

_log = logging.getLogger(__name__)
_user_log = logging.getLogger(os.environ.get("OPENEO_UDF_USER_LOGGER", f"{__name__}.user"))


def inspect(data=None, message: str = "", code: str = "User", level: str = "info"):
    """
    Implementation of the openEO `inspect` process for UDF contexts.

    Note that it is up to the back-end implementation to properly capture this logging
    and include it in the batch job logs.

    :param data: data to log
    :param message: message to send in addition to the data
    :param code: A label to help identify one or more log entries
    :param level: The severity level of this message. Allowed values: "error", "warning", "info", "debug"

    .. versionadded:: 0.10.1

    .. seealso:: :ref:`udf_logging_with_inspect`
    """
    extra = {"data": data, "code": code}
    kwargs = {"stacklevel": 2} if sys.version_info >= (3, 8) else {}
    _user_log.log(level=logging.getLevelName(level.upper()), msg=message, extra=extra, **kwargs)

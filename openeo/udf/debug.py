"""
Debug utilities for UDFs
"""
import json
import logging
import os

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
    """
    # The openEO logging API defines separate fields like `data` and `message` field,
    # but with the python logging API we can just have a single string,
    # so we JSON-encode our data to a single payload.
    msg = json.dumps({"data": data, "message": message, "code": code})
    _user_log.log(level=logging.getLevelName(level.upper()), msg=msg, stacklevel=2)

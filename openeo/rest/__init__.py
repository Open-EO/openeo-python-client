from typing import Optional

from openeo import BaseOpenEoException

# TODO: get from config file
DEFAULT_DOWNLOAD_CHUNK_SIZE = 10_000_000  # 10MB


class OpenEoClientException(BaseOpenEoException):
    """Base class for OpenEO client exceptions"""
    pass


class CapabilitiesException(OpenEoClientException):
    """Back-end does not support certain openEO feature or endpoint."""


class JobFailedException(OpenEoClientException):
    """A synchronous batch job failed. This exception references its corresponding job so the client can e.g.
    retrieve its logs.
    """

    def __init__(self, message, job):
        super().__init__(message)
        self.job = job


class OperatorException(OpenEoClientException):
    """Invalid (mathematical) operator usage."""
    pass


class BandMathException(OperatorException):
    """Invalid "band math" usage."""
    pass


class OpenEoRestError(OpenEoClientException):
    pass


class OpenEoApiPlainError(OpenEoRestError):
    """
    Base class for openEO API error responses, not necessarily following the openEO API specification
    (e.g. not properly JSON encoded, missing required fields, ...)

    :param message: the direct error message from the response
    :param http_status_code: the HTTP status code of the response
    :param error_message: the error message to show when the exception is rendered
        (by default a combination of the HTTP status code and the message)

    .. versionadded:: 0.25.0
    """

    __slots__ = ("http_status_code", "message")

    def __init__(
        self,
        message: str,
        *,
        http_status_code: Optional[int] = None,
        error_message: Optional[str] = None,
    ):
        super().__init__(error_message or f"[{http_status_code}] {message}")
        self.http_status_code = http_status_code
        self.message = message


class OpenEoApiError(OpenEoApiPlainError):
    """
    Exception for API error responses following the openEO API specification
    (https://api.openeo.org/#section/API-Principles/Error-Handling):
    JSON-encoded body, some expected fields like "code" and "message", ...
    """

    __slots__ = ("http_status_code", "code", "message", "id", "url")

    def __init__(
        self,
        *,
        http_status_code: int,
        code: str,
        message: str,
        id: Optional[str] = None,
        url: Optional[str] = None,
    ):
        super().__init__(
            message=message,
            http_status_code=http_status_code,
            error_message=f"[{http_status_code}] {code}: {message}" + (f" (ref: {id})" if id else ""),
        )
        self.http_status_code = http_status_code
        self.code = code
        self.message = message
        self.id = id
        self.url = url

from openeo import BaseOpenEoException


class OpenEoClientException(BaseOpenEoException):
    """Base class for OpenEO client exceptions"""
    pass


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


class OpenEoApiError(OpenEoRestError):
    """
    Error returned by OpenEO API according to https://open-eo.github.io/openeo-api/errors/
    """

    def __init__(self, http_status_code: int = None,
                 code: str = 'unknown', message: str = 'unknown error', id: str = None, url: str = None):
        self.http_status_code = http_status_code
        self.code = code
        self.message = message
        self.id = id
        self.url = url
        msg = "[{s}] {c}: {m}".format(s=self.http_status_code, c=self.code, m=self.message)
        if self.id:
            msg += " (ref: {i})".format(i=self.id)
        super().__init__(msg)

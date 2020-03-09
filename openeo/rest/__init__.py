class OpenEoClientException(Exception):
    """Base class for OpenEO client exceptions"""
    pass


class JobFailedException(OpenEoClientException):
    """A synchronous batch job failed. This exception references its corresponding job so the client can e.g.
    retrieve its logs.
    """
    def __init__(self, message, job):
        super().__init__(message)
        self.job = job

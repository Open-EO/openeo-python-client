# TODO: this seems to be driver specific: better move this to openeo-python-driver?

class ErrorSummary:
    def __init__(self, exception: Exception, is_client_error: bool, summary: str = None):
        self.exception = exception
        self.is_client_error = is_client_error
        self.summary = summary or str(exception)

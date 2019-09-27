# TODO: this seems to be driver specific: better move this to openeo-python-driver?

class ErrorSummary:
    def __init__(self, exception: Exception, is_client_error: bool, summary: str = None):
        self.exception = exception
        self.is_client_error = is_client_error
        self.summary = summary or str(exception)

    def __str__(self):
        return str({
            'exception': "%s: %s" % (type(self.exception).__name__, self.exception),
            'is_client_error': self.is_client_error,
            'summary': self.summary
        })

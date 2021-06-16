import json

class LogEntry:
    """
    A log entry.
    """

    def __init__(self, entry):
        # Unique ID for the log, string, REQUIRED
        self.id = entry['id']

        # Error code, string, optional
        self.code = entry.get("code", "")

        # Severity level, string (error, warning, info or debug), REQUIRED
        self.level = entry['level']

        # Error message, string, REQUIRED
        self.message = entry['message']

        # Date and time of the error event as RFC3339 date-time, string, available since API 1.1.0
        # todo: Use native date/time object?
        self.time =  entry.get("time", None)

        # A "stack trace" for the process, array of dicts
        self.path = entry.get("path", [])

        # Related links, array of dicts
        self.links = entry.get("links", [])

        # Usage metrics, dict, available since API 1.1.0
        # May contain the following metrics: cpu, memory, duration, network, disk, storage and other custom ones
        # Each of the metrics is also a dict with the following parts: value (numeric) and unit (string)
        self.usage = entry.get("usage", {})
        
        # Arbritrary data the user wants to "log" for debugging purposes.
        # Please note that this property may not exist as there's a difference
        # between None and non-existing. None for example refers to no-data in
        # many cases while the absence of the property means that the user did 
        # not provide any data for debugging.
        if 'data' in entry:
            self.data = entry['data']
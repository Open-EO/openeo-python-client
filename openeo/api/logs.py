"""
LogEntry - log messages for jobs and services

Properties:
- id: Unique ID for the log, string, REQUIRED
- code: Error code, string, optional
- level: Severity level, string (error, warning, info or debug), REQUIRED
- message: Error message, string, REQUIRED
- time: Date and time of the error event as RFC3339 date-time, string, available since API 1.1.0
- path: A "stack trace" for the process, array of dicts
- links: Related links, array of dicts
- usage: Usage metrics available as property 'usage', dict, available since API 1.1.0
  May contain the following metrics: cpu, memory, duration, network, disk, storage and other custom ones
  Each of the metrics is also a dict with the following parts: value (numeric) and unit (string)
- data: Arbritrary data the user wants to "log" for debugging purposes.
  Please note that this property may not exist as there's a difference
  between None and non-existing. None for example refers to no-data in
  many cases while the absence of the property means that the user did 
  not provide any data for debugging.
"""

class LogEntry(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Check required fields
        missing = self._required.difference(self.keys())
        if missing:
            raise ValueError("Missing required fields: {m}".format(m=sorted(missing)))
        
        # todo: Use native date/time object?
        self.setdefault("time", None)
        self.setdefault("links", [])

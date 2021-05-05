import json

class LogEntry:
    """
    A log entry.
    """

    def __init__(self, entry):
        self.log_id = entry['id']
        self.code = entry.get("code", 0)
        self.level = entry['level']
        self.message = entry['message']
        self.time =  entry.get("time", None) # todo: native date/time object?
        self.path = entry.get("path", [])
        self.links = entry.get("links", [])
        if 'data' in entry:
            self.data = entry['data']

    def toJson(self):
        return json.dumps(self, default = lambda o: o.__dict__) # doesn't work
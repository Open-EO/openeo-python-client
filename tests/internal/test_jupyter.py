from openeo.internal.jupyter import render_component
from openeo.rest.models.logs import LogEntry


def test_render_component_logs():
    entries = [LogEntry({"id": "log-01", "level": "info", "message": "hello"})]
    html = render_component("logs", data=entries)
    # TODO: smarter html checks?
    assert '"id": "log-01"' in html
    assert '"level": "info"' in html
    assert '"message": "hello"' in html

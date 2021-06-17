from openeo.api.logs import LogEntry
import pytest


def test_log_entry_empty():
    with pytest.raises(ValueError, match="Missing required fields"):
        _ = LogEntry()


def test_log_entry_basic_kwargs():
    log = LogEntry(id="log01", level="info", message="hello")
    assert log["id"] == "log01"
    assert log.id == "log01"
    assert log["level"] == "info"
    assert log.level == "info"
    assert log["message"] == "hello"
    assert log.message == "hello"


def test_log_entry_basic_dict():
    data = {"id": "log01", "level": "info", "message": "hello"}
    log = LogEntry(data)
    assert log["id"] == "log01"
    assert log.id == "log01"
    assert log["level"] == "info"
    assert log.level == "info"
    assert log["message"] == "hello"
    assert log.message == "hello"


def test_log_entry_legacy():
    log = LogEntry(id="log01", level="info", message="hello")
    assert log.log_id == "log01"

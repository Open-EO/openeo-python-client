import logging

import pytest

from openeo.rest.models.logs import LogEntry, log_level_name, normalize_log_level


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


@pytest.mark.parametrize(
    ["log_level_in", "expected_log_level"],
    [
        (None, logging.DEBUG),
        ("", logging.DEBUG),
        (logging.ERROR, logging.ERROR),
        ("error", logging.ERROR),
        ("ERROR", logging.ERROR),
        (logging.WARNING, logging.WARNING),
        ("warning", logging.WARNING),
        ("WARNING", logging.WARNING),
        (logging.INFO, logging.INFO),
        ("INFO", logging.INFO),
        ("info", logging.INFO),
        (logging.DEBUG, logging.DEBUG),
        ("DEBUG", logging.DEBUG),
        ("debug", logging.DEBUG),
        ("m3h!", logging.DEBUG),
    ],
)
def test_normalize_log_level(log_level_in, expected_log_level):
    assert normalize_log_level(log_level_in) == expected_log_level


def test_normalize_log_level_default():
    assert normalize_log_level(None) == logging.DEBUG
    assert normalize_log_level(None, default=logging.ERROR) == logging.ERROR


@pytest.mark.parametrize("log_level", [10.0, b"not a string"])
def test_normalize_log_level_raises_type_error(log_level):
    with pytest.raises(TypeError):
        assert normalize_log_level(log_level)


def test_log_level_name():
    assert log_level_name("debug") == "debug"
    assert log_level_name("DEBUG") == "debug"
    assert log_level_name("info") == "info"
    assert log_level_name("InfO") == "info"
    assert log_level_name("warn") == "warning"
    assert log_level_name("warning") == "warning"
    assert log_level_name("Warning") == "warning"
    assert log_level_name("error") == "error"
    assert log_level_name("ERROR") == "error"

    assert log_level_name(logging.DEBUG) == "debug"
    assert log_level_name(logging.INFO) == "info"
    assert log_level_name(logging.WARN) == "warning"
    assert log_level_name(logging.WARNING) == "warning"
    assert log_level_name(logging.ERROR) == "error"

    assert log_level_name(None) == "debug"

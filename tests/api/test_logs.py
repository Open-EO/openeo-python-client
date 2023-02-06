import logging

from openeo.api.logs import LogEntry, normalize_log_level, string_to_log_level
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
    ],
)
def test_normalize_log_level(log_level_in, expected_log_level):
    assert normalize_log_level(log_level_in) == expected_log_level


@pytest.mark.parametrize("log_level", [10.0, b"not a string"])
def test_normalize_log_level_raises_type_error(log_level):
    with pytest.raises(TypeError):
        assert normalize_log_level(log_level)


@pytest.mark.parametrize(
    ["log_level_in", "expected_log_level"],
    [
        ("", logging.DEBUG),
        ("error", logging.ERROR),
        ("ERROR", logging.ERROR),
        ("warning", logging.WARNING),
        ("WARNING", logging.WARNING),
        ("INFO", logging.INFO),
        ("info", logging.INFO),
        ("DEBUG", logging.DEBUG),
        ("debug", logging.DEBUG),
    ],
)
def test_string_to_log_level(log_level_in, expected_log_level):
    assert string_to_log_level(log_level_in) == expected_log_level


@pytest.mark.parametrize(
    "log_level", [None, 42, 10.0, logging.WARNING, b"not a string"]
)
def test_string_to_log_level_raises_type_error(log_level):
    with pytest.raises(TypeError):
        assert string_to_log_level(log_level)

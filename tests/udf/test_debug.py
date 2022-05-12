import logging
from pathlib import Path

from openeo.udf.debug import inspect


def test_inspect_basic(caplog):
    caplog.set_level("INFO")
    inspect(data=[1, 2, 3], message="hello")

    record: logging.LogRecord = caplog.records[0]
    assert record.name == "openeo.udf.debug.user"
    assert record.levelno == logging.INFO
    assert "hello" in record.message
    assert "[1, 2, 3]" in record.message
    assert record.filename == Path(__file__).name


def test_inspect_warning(caplog):
    caplog.set_level("INFO")
    inspect(data=[1, 2, 3], message="hello", level="warning")
    record: logging.LogRecord = caplog.records[0]
    assert record.levelno == logging.WARNING


def test_inspect_code(caplog):
    caplog.set_level("INFO")
    inspect(data=[1, 2, 3], message="hello", code="Dev")
    record: logging.LogRecord = caplog.records[0]
    assert "Dev" in record.message

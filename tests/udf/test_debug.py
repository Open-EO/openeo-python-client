import logging
import sys
from pathlib import Path

import pytest

from openeo.udf.debug import inspect


def test_inspect_basic(caplog):
    caplog.set_level("INFO")
    inspect(data=[1, 2, 3], message="hello")

    record: logging.LogRecord = caplog.records[0]
    assert record.name == "openeo.udf.debug.user"
    assert record.levelno == logging.INFO
    assert record.message == "hello"
    assert record.__dict__["data"] == [1, 2, 3]
    assert record.__dict__["code"] == "User"


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires python 3.8 or higher (logging `stacklevel`)")
def test_inspect_filename(caplog):
    caplog.set_level("INFO")
    inspect(data=[1, 2, 3], message="hello")
    record: logging.LogRecord = caplog.records[0]
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
    assert record.__dict__["code"] == "Dev"

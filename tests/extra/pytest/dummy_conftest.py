import re

import pytest

import openeo
from openeo.rest._testing import DummyBackend

pytest_plugins = ["openeo.extra.pytest.auto_list_job_ids"]


# Use the DummyBackend machinery to set up
# a minimally functional dummy backend for testing
@pytest.fixture
def dummy_backend(requests_mock):
    dummy = DummyBackend.at_url("https://oeo.test/", requests_mock=requests_mock)
    dummy.job_id_generator = _job_id_generator
    return dummy


def _job_id_generator(process_graph: dict) -> str:
    """build job id from digits extracted from the serialization of the given process graph"""
    return "job-" + re.sub(r"[^0-9]+", "", repr(process_graph))


@pytest.fixture
def dummy_api_url(dummy_backend):
    return dummy_backend.connection.root_url


@pytest.fixture
def connection(dummy_api_url, auto_list_job_ids):
    con = openeo.connect(dummy_api_url)
    auto_list_job_ids(con)
    return con

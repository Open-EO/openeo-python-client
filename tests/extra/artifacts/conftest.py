from typing import Iterator

import pytest

from openeo import Connection
from tests.rest.conftest import API_URL


@pytest.fixture
def extra_api_capabilities() -> dict:
    """
    Fixture to be overridden for customizing the capabilities doc used by connection fixtures.
    To be used as kwargs for `build_capabilities`
    """
    return {}


@pytest.fixture
def conn_with_extra_capabilities(requests_mock, extra_api_capabilities) -> Iterator[Connection]:
    requests_mock.get(API_URL, json={"api_version": "1.0.0", **extra_api_capabilities})
    yield Connection(API_URL)


@pytest.fixture
def clean_capabilities_cache() -> Iterator[None]:
    from openeo.extra.artifacts._backend import _capabilities_cache

    _capabilities_cache.clear()
    yield

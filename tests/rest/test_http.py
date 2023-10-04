import logging

import pytest
import requests.exceptions
from re_assert import Matches

from openeo.rest.http import requests_with_retry


def test_requests_with_retry(caplog):
    """Simple test for retrying using an invalid domain."""
    caplog.set_level(logging.DEBUG)

    session = requests_with_retry(total=2, backoff_factor=0.1)
    with pytest.raises(requests.exceptions.ConnectionError, match="Max retries exceeded"):
        _ = session.get("https://example.test")

    assert caplog.messages == [
        "Starting new HTTPS connection (1): example.test:443",
        Matches("Incremented Retry.*Retry\(total=1"),
        # Matches("Retrying.*total=1.*Failed to establish a new connection"),
        Matches("Retrying.*total=1.*Failed to resolve 'example.test'"),
        "Starting new HTTPS connection (2): example.test:443",
        Matches("Incremented Retry.*Retry\(total=0"),
        Matches("Retrying.*total=0.*Failed to resolve 'example.test'"),
        "Starting new HTTPS connection (3): example.test:443",
    ]


def test_requests_with_retry_zero(caplog):
    """Simple test for retrying using an invalid domain."""
    caplog.set_level(logging.DEBUG)

    session = requests_with_retry(total=0)
    with pytest.raises(requests.exceptions.ConnectionError, match="Max retries exceeded"):
        _ = session.get("https://example.test")

    assert caplog.messages == [
        "Starting new HTTPS connection (1): example.test:443",
    ]

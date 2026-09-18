from pathlib import Path
from unittest import mock

import httpretty
import pytest

from openeo.rest._connection import RestApiConnection
from openeo.utils.http import session_with_retries


class TestDownloadRangedStandardRetry:
    """
    Regression test for #934: `_download_ranged` no longer implements its own
    retry loop; transient failures are retried by the standard urllib3 Retry
    configuration of the connection's session.
    """

    @pytest.fixture(autouse=True)
    def _auto_httpretty_enabled(self):
        with httpretty.enabled(allow_net_connect=False):
            yield

    def test_transient_503_on_range_is_retried_by_session(self, tmp_path, time_sleep):
        content = b"0123456789" * 100  # 1000 bytes -> two 500-byte ranges
        url = "https://example.test/dl/file.bin"

        httpretty.register_uri(
            httpretty.GET,
            uri=url,
            responses=[
                # First attempt on range 0-499: transient failure.
                httpretty.Response(status=503, body="Service Unavailable"),
                # Retry of range 0-499 succeeds.
                httpretty.Response(
                    status=206,
                    body=content[0:500],
                    adding_headers={"Content-Range": f"bytes 0-499/{len(content)}"},
                ),
                # Range 500-999 succeeds on the first attempt.
                httpretty.Response(
                    status=206,
                    body=content[500:],
                    adding_headers={"Content-Range": f"bytes 500-999/{len(content)}"},
                ),
            ],
        )

        connection = RestApiConnection(root_url="https://example.test", session=session_with_retries())
        target = tmp_path / "file.bin"
        connection._download_ranged(url=url, target=target, file_size=len(content), range_size=500)

        assert target.read_bytes() == content

    @pytest.fixture
    def time_sleep(self):
        with mock.patch("time.sleep") as m:
            yield m

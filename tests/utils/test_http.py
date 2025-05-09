import contextlib
import logging
from typing import Iterator
from unittest import mock

import httpretty
import pytest
import requests

from openeo.utils.http import session_with_retries


class TestSessionWithRetries:
    @pytest.fixture(autouse=True)
    def time_sleep(self) -> Iterator[mock.Mock]:
        with mock.patch("time.sleep") as mock_sleep:
            yield mock_sleep

    @pytest.fixture(autouse=True)
    def _auto_httpretty_enabled(self):
        """Automatically activate httpretty for all tests in this class."""
        with httpretty.enabled(allow_net_connect=False):
            yield

    def test_default_basic(self, time_sleep):
        responses = [
            httpretty.Response(status=429, body="Stop it!"),
            httpretty.Response(status=200, body="ok then"),
        ]
        httpretty.register_uri(httpretty.GET, uri="https://example.test/", responses=responses)
        session = session_with_retries()
        resp = session.get("https://example.test/")
        assert resp.status_code == 200
        assert resp.text == "ok then"
        # Single retry does not trigger a sleep
        assert time_sleep.call_args_list == []

    @pytest.mark.parametrize(
        ["fail_count", "expected_sleeps", "success"],
        [
            (0, [], True),
            (1, [], True),
            (2, [5], True),
            (3, [5, 10], True),
            (5, [5, 10, 20, 40], True),
            (6, [5, 10, 20, 40], False),
        ],
    )
    def test_default_multiple_attempts(self, time_sleep, fail_count, expected_sleeps, success):
        responses = [httpretty.Response(status=429, body="Stop it!")] * fail_count
        responses.append(httpretty.Response(status=200, body="ok then"))
        httpretty.register_uri(httpretty.GET, uri="https://example.test/", responses=responses)
        session = session_with_retries()

        try:
            result = session.get("https://example.test/")
        except Exception as e:
            result = e

        if success:
            assert isinstance(result, requests.Response)
            assert result.status_code == 200
            assert result.text == "ok then"
        else:
            assert isinstance(result, requests.exceptions.RetryError)

        assert time_sleep.call_args_list == [mock.call(s) for s in expected_sleeps]

    @pytest.mark.parametrize(
        ["retry_config", "responses", "expected", "expected_sleeps"],
        [
            (
                # Fail with 500, which is in status_forcelist -> keep trying
                {"total": 3, "backoff_factor": 1.1, "status_forcelist": [500, 502, 503]},
                [
                    httpretty.Response(status=500, body="Internal Server Error"),
                    httpretty.Response(status=500, body="Internal Server Error"),
                    httpretty.Response(status=500, body="Internal Server Error"),
                ],
                requests.exceptions.RetryError,
                [2.2, 4.4],
            ),
            (
                # Fail with 500, not in status_forcelist -> no retrying
                {"total": 3, "backoff_factor": 1.1, "status_forcelist": [502, 503]},
                [
                    httpretty.Response(status=500, body="Internal Server Error"),
                    httpretty.Response(status=500, body="Internal Server Error"),
                    httpretty.Response(status=500, body="Internal Server Error"),
                ],
                (500, "Internal Server Error"),
                [],
            ),
            (
                # Multiple statuses in status_forcelist, retry until success
                {"total": 3, "backoff_factor": 1.1, "status_forcelist": [500, 502, 503]},
                [
                    httpretty.Response(status=500, body="Internal Server Error"),
                    httpretty.Response(status=502, body="Bad Gateway"),
                    httpretty.Response(status=503, body="Service Unavailable"),
                    httpretty.Response(status=200, body="Ok then"),
                ],
                (200, "Ok then"),
                [2.2, 4.4],
            ),
        ],
    )
    def test_custom_retries(self, time_sleep, retry_config, responses, expected, expected_sleeps):
        httpretty.register_uri(httpretty.GET, uri="https://example.test/", responses=responses)
        session = session_with_retries(retry=retry_config)

        try:
            result = session.get("https://example.test/")
        except Exception as e:
            result = e

        if isinstance(expected, type):
            assert isinstance(result, expected)
        elif isinstance(expected, tuple):
            assert (result.status_code, result.text) == expected
        else:
            raise ValueError(expected)

        assert time_sleep.call_args_list == [mock.call(s) for s in expected_sleeps]

        return

    @pytest.mark.parametrize(
        ["retry_config"],
        [
            (None,),
            # Retry-after is even honored when 429 is not in status_forcelist
            ({"status_forcelist": []},),
        ],
    )
    def test_retry_after(self, time_sleep, retry_config):
        """
        Test that the Retry-After header is respected.
        """
        responses = [
            httpretty.Response(status=429, body="Stop it!", adding_headers={"Retry-After": "23"}),
            httpretty.Response(status=200, body="ok then"),
        ]
        httpretty.register_uri(httpretty.GET, uri="https://example.test/", responses=responses)
        session = session_with_retries(retry_config)
        resp = session.get("https://example.test/")
        assert resp.status_code == 200
        assert resp.text == "ok then"
        assert time_sleep.call_args_list == [mock.call(23)]

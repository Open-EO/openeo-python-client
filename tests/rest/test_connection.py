import json
import logging
import os
import random
import re
import textwrap
import typing
import unittest.mock as mock
import zlib
from contextlib import nullcontext
from pathlib import Path

import pytest
import requests.auth
import requests_mock
import shapely.geometry

import openeo
from openeo.capabilities import ApiVersionException
from openeo.internal.graph_building import FlatGraphableMixin, PGNode
from openeo.rest import (
    CapabilitiesException,
    OpenEoApiError,
    OpenEoApiPlainError,
    OpenEoClientException,
    OpenEoRestError,
)
from openeo.rest._testing import build_capabilities
from openeo.rest.auth.auth import BearerAuth, NullAuth
from openeo.rest.auth.oidc import OidcException
from openeo.rest.auth.testing import ABSENT, OidcMock
from openeo.rest.connection import (
    DEFAULT_TIMEOUT,
    DEFAULT_TIMEOUT_SYNCHRONOUS_EXECUTE,
    Connection,
    RestApiConnection,
    connect,
    paginate,
)
from openeo.rest.vectorcube import VectorCube
from openeo.testing.stac import StacDummyBuilder
from openeo.util import ContextTimer, dict_no_none

from .auth.test_cli import auth_config, refresh_token_store

API_URL = "https://oeo.test/"

BASIC_ENDPOINTS = [{"path": "/credentials/basic", "methods": ["GET"]}]

# Trick to avoid linting/auto-formatting tools to complain about or fix unused imports of these pytest fixtures
auth_config = auth_config
refresh_token_store = refresh_token_store


@pytest.mark.parametrize(
    ["base", "paths", "expected_path"],
    [
        # Simple
        ("https://oeo.test", ["foo", "/foo"], "https://oeo.test/foo"),
        ("https://oeo.test/", ["foo", "/foo"], "https://oeo.test/foo"),
        # With trailing slash
        ("https://oeo.test", ["foo/", "/foo/"], "https://oeo.test/foo/"),
        ("https://oeo.test/", ["foo/", "/foo/"], "https://oeo.test/foo/"),
        # Deeper
        ("https://oeo.test/api/v04", ["foo/bar", "/foo/bar"], "https://oeo.test/api/v04/foo/bar"),
        ("https://oeo.test/api/v04/", ["foo/bar", "/foo/bar"], "https://oeo.test/api/v04/foo/bar"),
        ("https://oeo.test/api/v04", ["foo/bar/", "/foo/bar/"], "https://oeo.test/api/v04/foo/bar/"),
        ("https://oeo.test/api/v04/", ["foo/bar/", "/foo/bar/"], "https://oeo.test/api/v04/foo/bar/"),
    ]
)
def test_rest_api_connection_url_handling(requests_mock, base, paths, expected_path):
    """Test connection __init__ and proper joining of root url and API path"""
    conn = RestApiConnection(base)
    requests_mock.get(expected_path, text="payload")
    requests_mock.post(expected_path, text="payload")
    for path in paths:
        assert conn.get(path).text == "payload"
        assert conn.post(path, {"foo": "bar"}).text == "payload"


def test_rest_api_headers():
    conn = RestApiConnection(API_URL)
    with requests_mock.Mocker() as m:
        def text(request, context):
            assert re.match(
                r"^openeo-python-client/[0-9a-z.-]+ .*python/3.* (linux|win|darwin)",
                request.headers["User-Agent"],
                re.I
            )
            assert request.headers["X-Openeo-Bar"] == "XY123"

        m.get("/foo", text=text)
        m.post("/foo", text=text)
        conn.get("/foo", headers={"X-Openeo-Bar": "XY123"})
        conn.post("/foo", {}, headers={"X-Openeo-Bar": "XY123"})


def test_rest_api_expected_status(requests_mock):
    conn = RestApiConnection(API_URL)
    requests_mock.get("https://oeo.test/foo", status_code=200, json={"o": "k"})
    # Expected status
    assert conn.get("/foo", expected_status=200).json() == {"o": "k"}
    assert conn.get("/foo", expected_status=[200, 201]).json() == {"o": "k"}
    # Unexpected status
    with pytest.raises(OpenEoClientException, match=r"Got status code 200 for `GET /foo` \(expected \[204\]\)"):
        conn.get("/foo", expected_status=204)
    with pytest.raises(OpenEoClientException, match=r"Got status code 200 for `GET /foo` \(expected \[203, 204\]\)"):
        conn.get("/foo", expected_status=[203, 204])


def test_rest_api_expected_status_with_error(requests_mock):
    conn = RestApiConnection(API_URL)
    requests_mock.get("https://oeo.test/bar", status_code=406, json={"code": "NoBar", "message": "no bar please"})
    # First check for API error by default
    with pytest.raises(OpenEoApiError, match=r"\[406\] NoBar: no bar please"):
        conn.get("/bar", expected_status=200)
    with pytest.raises(OpenEoApiError, match=r"\[406\] NoBar: no bar please"):
        conn.get("/bar", expected_status=[201, 202])
    # Don't fail when an error status is actually expected
    conn.get("/bar", expected_status=406)
    conn.get("/bar", expected_status=[406, 407])
    with pytest.raises(OpenEoApiError, match=r"\[406\] NoBar: no bar please"):
        conn.get("/bar", expected_status=[401, 402])

    # Don't check for error, just status
    conn.get("/bar", check_error=False, expected_status=406)
    with pytest.raises(OpenEoClientException, match=r"Got status code 406 for `GET /bar` \(expected \[302\]\)"):
        conn.get("/bar", check_error=False, expected_status=302)
    with pytest.raises(OpenEoClientException, match=r"Got status code 406 for `GET /bar` \(expected \[302, 303\]\)"):
        conn.get("/bar", check_error=False, expected_status=[302, 303])


@pytest.mark.parametrize(
    ["status_code", "text", "expected"],
    [
        (400, "Nope, no JSON here", "[400] Nope, no JSON here"),
        (400, '{"code": "InvalidJSON", "message": "oops}}}}}}', '[400] {"code": "InvalidJSON", "message": "oops}}}}}}'),
        (400, '{"status": "error", "msg": "Nope"}', '[400] {"status": "error", "msg": "Nope"}'),
        (500, '{"status": "error", "msg": "Nope"}', '[500] {"status": "error", "msg": "Nope"}'),
        (400, '{"status": "error", "code": "WrongColor"}', '[400] {"status": "error", "code": "WrongColor"}'),
        (400, '{"code": "WrongColor", "message": "Nope"}', "[400] WrongColor: Nope"),
        (400, '{"code": "WrongColor", "message": "Nope", "id": "r-123"}', "[400] WrongColor: Nope (ref: r-123)"),
    ],
)
def test_error_response_format(requests_mock, status_code, text, expected):
    requests_mock.get("https://oeo.test/bar", status_code=status_code, text=text)
    conn = RestApiConnection(API_URL)
    with pytest.raises(
        OpenEoApiPlainError,
        match=re.escape(expected) if isinstance(expected, str) else expected,
    ):
        conn.get("/bar")


def test_502_proxy_error(requests_mock):
    """EP-3387"""
    requests_mock.get("https://oeo.test/bar", status_code=502, text="""<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">
<html><head>
<title>502 Proxy Error</title>
</head><body>
<h1>Proxy Error</h1>
<p>The proxy server received an invalid
response from an upstream server.<br />
The proxy server could not handle the request <em><a href="/openeo/0.4.0/result">POST&nbsp;/openeo/0.4.0/result</a></em>.<p>
Reason: <strong>Error reading from remote server</strong></p></p>
</body></html>""")
    conn = RestApiConnection(API_URL)
    with pytest.raises(OpenEoApiPlainError, match="Consider.*batch job.*instead"):
        conn.get("/bar")


def test_cdse_firewall_error(requests_mock):
    """https://github.com/Open-EO/openeo-python-client/issues/512"""
    requests_mock.get(
        "https://oeo.test/bar",
        status_code=403,
        text='{"status": "error", "data": {"message": "This request was rejected due to a violation. Please consult with your administrator, and provide reference ID 128968"}}',
    )
    conn = RestApiConnection(API_URL)
    with pytest.raises(
        OpenEoApiPlainError,
        match=r"\[403\].*This request was rejected.*provide reference ID 128968",
    ):
        conn.get("/bar")


@pytest.mark.parametrize(["root", "internals", "externals"], [
    (
            "https://openeo.test",
            ["https://openeo.test", "https://openeo.test/", "https://openeo.test/foo"],
            ["http://openeo.test", "https://openeo.test.foo.com"]
    ),
    (
            "https://openeo.test/",
            ["https://openeo.test", "https://openeo.test/", "https://openeo.test/foo"],
            ["http://openeo.test", "https://openeo.test.foo.com"]
    ),
    (
            "https://openeo.test/openeo/1.0",
            ["https://openeo.test/openeo/1.0", "https://openeo.test/openeo/1.0/", "https://openeo.test/openeo/1.0/foo"],
            ["https://openeo.test/openeo/0.4"]
    ),
])
def test_rest_api_external_url(root, internals, externals):
    con = RestApiConnection(root)
    for url in internals:
        assert con._is_external(url) is False
    for url in externals:
        assert con._is_external(url) is True


@pytest.mark.parametrize(["api_root", "url"], [
    ("https://oeo.test", "https://evilcorp.test/download/hello.txt"),
    ("https://oeo.test", "https://oeo.test.evilcorp.test/download/hello.txt"),
    ("https://oeo.test", "http://oeo.test/download/hello.txt"),
    ("https://oeo.test/foo", "https://oeo.test/bar/hello.txt"),
])
def test_rest_api_other_domain_auth_headers(requests_mock, api_root, url):
    """https://github.com/Open-EO/openeo-python-client/issues/201"""
    secret = "!secret token!"

    def debug(request: requests.Request, context):
        return repr(("hello world", request.headers))

    requests_mock.get(url, text=debug)

    con = RestApiConnection(api_root, auth=BearerAuth(secret))
    res = con.get(url)
    assert "hello world" in res.text
    assert "User-Agent': 'openeo-python-client/" in res.text
    assert secret not in res.text
    assert "auth" not in res.text.lower()


def test_rest_api_get_redirect(requests_mock):
    requests_mock.get("http://oeo.test/foo", status_code=302, headers={"Location": "https://oeo.test/bar"})
    requests_mock.get("https://oeo.test/bar", status_code=200, json={"hello": "world"})
    con = RestApiConnection("http://oeo.test")
    res = con.get("/foo", expected_status=200)
    assert res.json() == {"hello": "world"}


def test_rest_api_post_redirect(requests_mock):
    requests_mock.post("http://oeo.test/foo", status_code=302, headers={"Location": "https://oeo.test/foo"})
    requests_mock.get("https://oeo.test/foo", status_code=200, json={"hello": "world"})
    requests_mock.post("https://oeo.test/foo", status_code=200, json={"hello": "world"})
    con = RestApiConnection("http://oeo.test")
    with pytest.raises(OpenEoClientException, match=r"Got status code 302 for `POST /foo` \(expected \[200\]\)"):
        con.post("/foo", expected_status=200)


def test_slow_response_threshold(requests_mock, caplog):
    caplog.set_level(logging.WARNING)
    requests_mock.get("https://oeo.test/foo", status_code=200, text="hello world")
    con = RestApiConnection("https://oeo.test", slow_response_threshold=3)

    # Fast enough
    with mock.patch.object(ContextTimer, "_clock", new=iter([10, 11]).__next__):
        caplog.clear()
        assert con.get("/foo").text == "hello world"
    assert caplog.text == ""

    # Too slow
    with mock.patch.object(ContextTimer, "_clock", new=iter([10, 15]).__next__):
        caplog.clear()
        assert con.get("/foo").text == "hello world"
    assert "Slow response: `GET https://oeo.test/foo` took 5.00s (>3.00s)" in caplog.text

    # Custom threshold
    with mock.patch.object(ContextTimer, "_clock", new=iter([10, 15]).__next__):
        caplog.clear()
        assert con.get("/foo", slow_response_threshold=10).text == "hello world"
    assert caplog.text == ""


def test_slow_response_threshold_truncate_url(requests_mock, caplog):
    caplog.set_level(logging.WARNING)
    requests_mock.get("https://oeo.test/foo", status_code=200, text="hello world")
    con = RestApiConnection("https://oeo.test", slow_response_threshold=3)

    # Long URL
    with mock.patch.object(ContextTimer, "_clock", new=iter([10, 15]).__next__):
        caplog.clear()
        assert con.get("/foo?bar=" + ("0123456789" * 10)).text == "hello world"
    expected = "Slow response: `GET https://oeo.test/foo?bar=012345678901234567890123456789012345...` took 5.00s (>3.00s)"
    assert expected in caplog.text


def test_connection_other_domain_auth_headers(requests_mock, api_version):
    """https://github.com/Open-EO/openeo-python-client/issues/201"""
    secret = "!secret token!"

    def debug(request: requests.Request, context):
        return repr(("hello world", request.headers))

    requests_mock.get(API_URL, json={"api_version": api_version, "endpoints": BASIC_ENDPOINTS})
    requests_mock.get(API_URL + 'credentials/basic', json={"access_token": secret})
    requests_mock.get("https://evilcorp.test/download/hello.txt", text=debug)

    con = Connection(API_URL).authenticate_basic("john", "j0hn")
    res = con.get("https://evilcorp.test/download/hello.txt")
    assert "hello world" in res.text
    assert "User-Agent': 'openeo-python-client/" in res.text
    assert secret not in res.text
    assert "auth" not in res.text.lower()


def test_connection_default_https(requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    con = Connection("oeo.test")
    assert con.capabilities().api_version() == "1.0.0"


def test_connection_with_session():
    session = mock.Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {"foo": "bar", "api_version": "1.0.0"}
    conn = Connection("https://oeo.test/", session=session)
    assert conn.capabilities().capabilities["foo"] == "bar"
    session.request.assert_any_call(
        url="https://oeo.test/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY, timeout=DEFAULT_TIMEOUT
    )


def test_connect_with_session():
    session = mock.Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {"foo": "bar", "api_version": "1.0.0"}
    conn = connect("https://oeo.test/", session=session)
    assert conn.capabilities().capabilities["foo"] == "bar"
    session.request.assert_any_call(
        url="https://oeo.test/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY, timeout=DEFAULT_TIMEOUT
    )


@pytest.mark.parametrize(["versions", "expected_url", "expected_version"], [
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.test/openeo/0.4.1/"},
                {"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.0.0/"},
                {"api_version": "1.1.0", "url": "https://oeo.test/openeo/1.1.0/"},
            ],
            "https://oeo.test/openeo/1.1.0/",
            "1.1.0",
        ),
        (
            [
                {"api_version": "0.4.1", "url": "https://oeo.test/openeo/0.4.1/"},
                {"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.0.0/"},
                {
                    "api_version": "1.1.0",
                    "url": "https://oeo.test/openeo/1.1.0/",
                    "production": False,
                },
            ],
            "https://oeo.test/openeo/1.0.0/",
            "1.0.0",
    ),
    (
            [
                {
                    "api_version": "0.4.1",
                    "url": "https://oeo.test/openeo/0.4.1/",
                    "production": True,
                },
                {
                    "api_version": "1.0.0",
                    "url": "https://oeo.test/openeo/1.0.0/",
                    "production": True,
                },
                {
                    "api_version": "1.1.0",
                    "url": "https://oeo.test/openeo/1.1.0/",
                    "production": False,
                },
            ],
            "https://oeo.test/openeo/1.0.0/",
            "1.0.0",
    ),
    (
            [
                {
                    "api_version": "0.4.1",
                    "url": "https://oeo.test/openeo/0.4.1/",
                    "production": False,
                },
                {
                    "api_version": "1.0.0",
                    "url": "https://oeo.test/openeo/1.0.0/",
                    "production": False,
                },
                {
                    "api_version": "1.1.0",
                    "url": "https://oeo.test/openeo/1.1.0/",
                    "production": False,
                },
            ],
            "https://oeo.test/openeo/1.1.0/",
            "1.1.0",
        ),
        (
            [
                {
                    "api_version": "0.1.0",
                    "url": "https://oeo.test/openeo/0.1.0/",
                    "production": True,
                },
                {
                    "api_version": "1.0.0",
                    "url": "https://oeo.test/openeo/1.0.0/",
                    "production": False,
                },
            ],
            "https://oeo.test/openeo/1.0.0/",
            "1.0.0",
    ),
    (
            [],
            "https://oeo.test/",
            "1.0.0",
    ),
])
def test_connect_version_discovery(requests_mock, versions, expected_url, expected_version):
    requests_mock.get("https://oeo.test/", status_code=404)
    requests_mock.get("https://oeo.test/.well-known/openeo", status_code=200, json={"versions": versions})
    requests_mock.get(expected_url, status_code=200, json={"foo": "bar", "api_version": expected_version})

    conn = connect("https://oeo.test/")
    assert conn.capabilities().capabilities["foo"] == "bar"


def test_connect_version_discovery_timeout(requests_mock):
    well_known = requests_mock.get("https://oeo.test/.well-known/openeo", status_code=200, json={
        "versions": [{"api_version": "1.0.0", "url": "https://oeo.test/v1/"}, ],
    })
    requests_mock.get("https://oeo.test/v1/", status_code=200, json={"api_version": "1.0.0"})

    _ = connect("https://oeo.test/")
    assert well_known.call_count == 1
    assert well_known.request_history[-1].timeout is None

    _ = connect("https://oeo.test/", default_timeout=4)
    assert well_known.call_count == 2
    assert well_known.request_history[-1].timeout == 4


def test_connect_api_version_too_old(requests_mock):
    requests_mock.get(API_URL, status_code=200, json={"api_version": "0.4.0"})
    with pytest.raises(ApiVersionException):
        _ = openeo.connect("https://oeo.test")


def test_connection_repr(requests_mock):
    requests_mock.get("https://oeo.test/", status_code=404)
    requests_mock.get("https://oeo.test/.well-known/openeo", status_code=200, json={
        "versions": [{"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.x/", "production": True}],
    })
    requests_mock.get("https://oeo.test/openeo/1.x/", status_code=200,
                      json={"api_version": "1.0.0", "endpoints": BASIC_ENDPOINTS})
    requests_mock.get("https://oeo.test/openeo/1.x/credentials/basic", json={"access_token": "w3lc0m3"})

    conn = connect("https://oeo.test/")
    assert repr(conn) == "<Connection to 'https://oeo.test/openeo/1.x/' with NullAuth>"
    conn.authenticate_basic("foo", "bar")
    assert repr(conn) == "<Connection to 'https://oeo.test/openeo/1.x/' with BasicBearerAuth>"


def test_capabilities_api_version(requests_mock):
    requests_mock.get(API_URL, status_code=200, json={"api_version": "1.0.0"})
    con = openeo.connect("https://oeo.test")
    capabilities = con.capabilities()
    assert capabilities.version() == "1.0.0"
    assert capabilities.api_version() == "1.0.0"


def test_capabilities_api_version_check(requests_mock):
    requests_mock.get(API_URL, status_code=200, json={"api_version": "1.2.3"})
    con = openeo.connect("https://oeo.test")
    api_version_check = con.capabilities().api_version_check
    assert api_version_check.below("1.2.4")
    assert api_version_check.below("1.1") is False
    assert api_version_check.at_most("1.3")
    assert api_version_check.at_most("1.2.3")
    assert api_version_check.at_most("1.2.2") is False
    assert api_version_check.at_least("1.2.3")
    assert api_version_check.at_least("1.5") is False
    assert api_version_check.above("1.2.3") is False
    assert api_version_check.above("1.2.2")


def test_capabilities_caching(requests_mock):
    m = requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    con = Connection(API_URL)
    assert con.capabilities().api_version() == "1.0.0"
    assert m.call_count == 1
    assert con.capabilities().api_version() == "1.0.0"
    assert m.call_count == 1


def test_file_formats(requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    m = requests_mock.get("https://oeo.test/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
    con = Connection(API_URL)
    assert con.list_file_formats() == {"output": {"GTiff": {"gis_data_types": ["raster"]}}}
    assert m.call_count == 1
    assert con.list_output_formats() == {"GTiff": {"gis_data_types": ["raster"]}}
    assert m.call_count == 1


def test_api_error(requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get(
        "https://oeo.test/collections/foobar",
        status_code=404,
        json={
            "code": "CollectionNotFound",
            "message": "No such thing as a collection 'foobar'",
            "id": "54321",
        },
    )
    with pytest.raises(OpenEoApiError) as exc_info:
        Connection(API_URL).describe_collection("foobar")
    exc = exc_info.value
    assert exc.http_status_code == 404
    assert exc.code == "CollectionNotFound"
    assert exc.message == "No such thing as a collection 'foobar'"
    assert exc.id == "54321"
    assert exc.url is None
    assert str(exc) == "[404] CollectionNotFound: No such thing as a collection 'foobar' (ref: 54321)"


def test_api_error_no_id(requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get(
        "https://oeo.test/collections/foobar",
        status_code=404,
        json={
            "code": "CollectionNotFound",
            "message": "No such thing as a collection 'foobar'",
        },
    )
    with pytest.raises(OpenEoApiError) as exc_info:
        Connection(API_URL).describe_collection("foobar")
    exc = exc_info.value
    assert exc.http_status_code == 404
    assert exc.code == "CollectionNotFound"
    assert exc.message == "No such thing as a collection 'foobar'"
    assert exc.id is None
    assert exc.url is None
    assert str(exc) == "[404] CollectionNotFound: No such thing as a collection 'foobar'"


def test_api_error_non_json(requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    requests_mock.get('https://oeo.test/collections/foobar', status_code=500, text="olapola")
    with pytest.raises(OpenEoApiPlainError) as exc_info:
        conn.describe_collection("foobar")
    exc = exc_info.value
    assert exc.http_status_code == 500
    assert exc.message == "olapola"


def _credentials_basic_handler(username, password, access_token="w3lc0m3"):
    # TODO: better reuse of this helper
    expected_auth = requests.auth._basic_auth_str(username=username, password=password)

    def handler(request, context):
        assert request.headers["Authorization"] == expected_auth
        return json.dumps({"access_token": access_token})

    return handler


def test_create_connection_lazy_auth_config(requests_mock, api_version):
    user, pwd = "john262", "J0hndo3"
    requests_mock.get(API_URL, json={"api_version": api_version, "endpoints": BASIC_ENDPOINTS})
    requests_mock.get(API_URL + 'credentials/basic', text=_credentials_basic_handler(user, pwd))

    with mock.patch('openeo.rest.connection.AuthConfig') as AuthConfig:
        # Don't create default AuthConfig when not necessary
        conn = Connection(API_URL)
        assert AuthConfig.call_count == 0
        conn.authenticate_basic(user, pwd)
        assert AuthConfig.call_count == 0
        # call `authenticate_basic` so that fallback AuthConfig is created/used lazily
        AuthConfig.return_value.get_basic_auth.return_value = (user, pwd)
        conn.authenticate_basic()
        assert AuthConfig.call_count == 1
        conn.authenticate_basic()
        assert AuthConfig.call_count == 1


def test_create_connection_lazy_refresh_token_store(requests_mock):
    user, pwd = "john262", "J0hndo3"
    client_id = "myclient"
    client_secret = "$3cr3t"
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="password",
        expected_client_id=client_id,
        expected_fields={"username": user, "password": pwd, "scope": "openid", "client_secret": client_secret},
        oidc_issuer=issuer,
    )

    with mock.patch('openeo.rest.connection.RefreshTokenStore') as RefreshTokenStore:
        conn = Connection(API_URL)
        assert RefreshTokenStore.call_count == 0
        # Create RefreshTokenStore lazily when necessary
        conn.authenticate_oidc_resource_owner_password_credentials(
            username=user, password=pwd, client_id=client_id, client_secret=client_secret, store_refresh_token=True
        )
        assert RefreshTokenStore.call_count == 1
        RefreshTokenStore.return_value.set_refresh_token.assert_called_with(
            issuer=issuer, client_id=client_id, refresh_token=oidc_mock.state["refresh_token"]
        )


def test_authenticate_basic_no_support(requests_mock, api_version):
    requests_mock.get(API_URL, json={"api_version": api_version, "endpoints": []})

    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    with pytest.raises(OpenEoClientException, match="does not support basic auth"):
        conn.authenticate_basic(username="john", password="j0hn")
    assert isinstance(conn.auth, NullAuth)


def test_authenticate_basic(requests_mock, api_version):
    user, pwd = "john262", "J0hndo3"
    requests_mock.get(API_URL, json={"api_version": api_version, "endpoints": BASIC_ENDPOINTS})
    requests_mock.get(API_URL + 'credentials/basic', text=_credentials_basic_handler(user, pwd))

    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_basic(username=user, password=pwd)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == "basic//w3lc0m3"


def test_authenticate_basic_from_config(requests_mock, api_version, auth_config):
    user, pwd = "john281", "J0hndo3"
    requests_mock.get(API_URL, json={"api_version": api_version, "endpoints": BASIC_ENDPOINTS})
    requests_mock.get(API_URL + 'credentials/basic', text=_credentials_basic_handler(user, pwd))
    auth_config.set_basic_auth(backend=API_URL, username=user, password=pwd)

    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_basic()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == "basic//w3lc0m3"


@pytest.mark.slow
def test_authenticate_oidc_authorization_code_100_single_implicit(requests_mock, caplog):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "fauth", "issuer": "https://fauth.test", "title": "Foo Auth", "scopes": ["openid", "im"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "im openid"},
        oidc_issuer="https://fauth.test",
        scopes_supported=["openid", "im"],
    )

    # With all this set up, kick off the openid connect flow
    caplog.set_level(logging.INFO)
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_authorization_code(client_id=client_id, webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/fauth/' + oidc_mock.state["access_token"]
    assert "No OIDC provider given, but only one available: 'fauth'. Using that one." in caplog.text


def test_authenticate_oidc_authorization_code_100_single_wrong_id(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "fauth", "issuer": "https://fauth.test", "title": "Foo Auth", "scopes": ["openid", "w"]}]
    })

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    with pytest.raises(OpenEoClientException, match=r"'nopenope' not available\. Should be one of \['fauth'\]\."):
        conn.authenticate_oidc_authorization_code(
            client_id=client_id, provider_id="nopenope", webbrowser_open=pytest.fail
        )


@pytest.mark.slow
def test_authenticate_oidc_authorization_code_100_multiple_no_given_id(requests_mock, caplog):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo Auth", "scopes": ["openid", "w"]},
            {"id": "bauth", "issuer": "https://bauth.test", "title": "Bar Auth", "scopes": ["openid", "w"]},
        ]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid w"},
        oidc_issuer="https://fauth.test",
        scopes_supported=["openid", "w"],
    )

    # With all this set up, kick off the openid connect flow
    caplog.set_level(logging.INFO)
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_authorization_code(client_id=client_id, webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/fauth/' + oidc_mock.state["access_token"]
    assert "No OIDC provider given. Using first provider 'fauth' as advertised by backend." in caplog.text


def test_authenticate_oidc_authorization_code_100_multiple_wrong_id(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo Auth", "scopes": ["openid", "w"]},
            {"id": "bauth", "issuer": "https://bauth.test", "title": "Bar Auth", "scopes": ["openid", "w"]},
        ]
    })

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    match = r"'lol' not available\. Should be one of \[('fauth', 'bauth'|'bauth', 'fauth')\]\."
    with pytest.raises(OpenEoClientException, match=match):
        conn.authenticate_oidc_authorization_code(client_id=client_id, provider_id="lol", webbrowser_open=pytest.fail)


@pytest.mark.slow
def test_authenticate_oidc_authorization_code_100_multiple_success(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo Auth", "scopes": ["openid", "mu"]},
            {"id": "bauth", "issuer": "https://bauth.test", "title": "Bar Auth", "scopes": ["openid", "mu"]},
        ]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "mu openid"},
        oidc_issuer="https://bauth.test",
        scopes_supported=["openid", "mu"],
    )

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_authorization_code(
        client_id=client_id, provider_id="bauth", webbrowser_open=oidc_mock.webbrowser_open
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/bauth/' + oidc_mock.state["access_token"]


@pytest.mark.slow
@pytest.mark.parametrize(
    ["store_refresh_token", "scopes_supported", "expected_scope"],
    [
        (False, ["openid", "email"], "openid"),
        (False, ["openid", "email", "offline_access"], "openid"),
        (True, ["openid", "email"], "openid"),
        (True, ["openid", "email", "offline_access"], "offline_access openid"),
    ]
)
def test_authenticate_oidc_auth_code_pkce_flow(requests_mock, store_refresh_token, scopes_supported, expected_scope):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": expected_scope},
        oidc_issuer=issuer,
        scopes_supported=scopes_supported,
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_authorization_code(
        client_id=client_id, webbrowser_open=oidc_mock.webbrowser_open, store_refresh_token=store_refresh_token
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    if store_refresh_token:
        refresh_token = oidc_mock.state["refresh_token"]
        assert refresh_token_store.mock_calls == [
            mock.call.set_refresh_token(client_id=client_id, issuer=issuer, refresh_token=refresh_token)
        ]
    else:
        assert refresh_token_store.mock_calls == []


@pytest.mark.slow
def test_authenticate_oidc_auth_code_pkce_flow_client_from_config(requests_mock, auth_config):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid"},
        oidc_issuer=issuer,
        scopes_supported=["openid"],
    )
    auth_config.set_oidc_client_config(backend=API_URL, provider_id="oi", client_id=client_id)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_authorization_code(webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


def test_authenticate_oidc_client_credentials(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
        oidc_issuer=issuer,
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_client_credentials(
        client_id=client_id, client_secret=client_secret
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []
    # Again but store refresh token
    conn.authenticate_oidc_client_credentials(client_id=client_id, client_secret=client_secret)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == "oidc/oi/" + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


def test_authenticate_oidc_client_credentials_client_from_config(requests_mock, auth_config):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
        oidc_issuer=issuer,
    )
    auth_config.set_oidc_client_config(
        backend=API_URL, provider_id="oi", client_id=client_id, client_secret=client_secret
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_client_credentials()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.parametrize(
    ["env_provider_id", "expected_provider_id"],
    [
        (None, "oi"),
        ("oi", "oi"),
        ("dc", "dc"),
    ],
)
def test_authenticate_oidc_client_credentials_client_from_env(
    requests_mock, monkeypatch, env_provider_id, expected_provider_id
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    monkeypatch.setenv("OPENEO_AUTH_CLIENT_ID", client_id)
    monkeypatch.setenv("OPENEO_AUTH_CLIENT_SECRET", client_secret)
    if env_provider_id:
        monkeypatch.setenv("OPENEO_AUTH_PROVIDER_ID", env_provider_id)
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {"id": "oi", "issuer": "https://oi.test", "title": "example", "scopes": ["openid"]},
                {"id": "dc", "issuer": "https://dc.test", "title": "example", "scopes": ["openid"]},
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_issuer=f"https://{expected_provider_id}.test",
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_client_credentials()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == f"oidc/{expected_provider_id}/" + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.parametrize(
    [
        "env_provider_id",
        "arg_provider_id",
        "expected_provider_id",
        "env_client_id",
        "arg_client_id",
        "expected_client_id",
    ],
    [
        (None, None, "oi", None, "aclient", "aclient"),
        (None, "dc", "dc", None, "aclient", "aclient"),
        ("dc", None, "dc", "eclient", None, "eclient"),
        ("oi", "dc", "dc", "eclient", "aclient", "aclient"),
    ],
)
def test_authenticate_oidc_client_credentials_client_precedence(
    requests_mock,
    monkeypatch,
    env_provider_id,
    arg_provider_id,
    expected_provider_id,
    env_client_id,
    arg_client_id,
    expected_client_id,
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_secret = "$3cr3t"
    if env_client_id:
        monkeypatch.setenv("OPENEO_AUTH_CLIENT_ID", env_client_id)
        monkeypatch.setenv("OPENEO_AUTH_CLIENT_SECRET", client_secret)
    if env_provider_id:
        monkeypatch.setenv("OPENEO_AUTH_PROVIDER_ID", env_provider_id)
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {"id": "oi", "issuer": "https://oi.test", "title": "example", "scopes": ["openid"]},
                {"id": "dc", "issuer": "https://dc.test", "title": "example", "scopes": ["openid"]},
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_issuer=f"https://{expected_provider_id}.test",
        expected_grant_type="client_credentials",
        expected_client_id=expected_client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_client_credentials(
        client_id=arg_client_id, client_secret=client_secret if arg_client_id else None, provider_id=arg_provider_id
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == f"oidc/{expected_provider_id}/" + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.parametrize(
    ["provider_id_arg", "provider_id_env", "provider_id_conf", "expected_provider_id"],
    [
        (None, None, None, "oidc1"),
        ("oidc2", None, None, "oidc2"),
        (None, "oidc2", None, "oidc2"),
        (None, None, "oidc2", "oidc2"),
        ("oidc2", "oidc3", "oidc4", "oidc2"),
        ("oidc2", None, "oidc4", "oidc2"),
        (None, "oidc3", "oidc4", "oidc3"),
    ],
)
def test_authenticate_oidc_client_credentials_client_multiple_provider_resolution(
    requests_mock, monkeypatch, auth_config, provider_id_arg, provider_id_env, provider_id_conf, expected_provider_id
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    monkeypatch.setenv("OPENEO_AUTH_CLIENT_ID", client_id)
    monkeypatch.setenv("OPENEO_AUTH_CLIENT_SECRET", client_secret)
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": f"oidc{p}",
                    "issuer": f"https://oidc{p}.test",
                    "title": "One",
                    "scopes": ["openid"],
                    "default_clients": [
                        {
                            "id": client_id,
                            "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"],
                        }
                    ],
                }
                for p in [1, 2, 3, 4]
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_issuer=f"https://{expected_provider_id}.test",
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
    )
    if provider_id_env:
        monkeypatch.setenv("OPENEO_AUTH_PROVIDER_ID", provider_id_env)
    assert auth_config.load() == {}
    if provider_id_conf:
        auth_config.set_oidc_client_config(backend=API_URL, provider_id=provider_id_conf, client_id=None)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_client_credentials(provider_id=provider_id_arg)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == f"oidc/{expected_provider_id}/" + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


def test_authenticate_oidc_resource_owner_password_credentials(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    username, password = "john", "j0hn"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="password",
        expected_client_id=client_id,
        expected_fields={
            "username": username, "password": password, "scope": "openid", "client_secret": client_secret
        },
        oidc_issuer=issuer,
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_resource_owner_password_credentials(
        client_id=client_id, username=username, password=password, client_secret=client_secret
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []
    # Again but store refresh token
    conn.authenticate_oidc_resource_owner_password_credentials(
        client_id=client_id, username=username, password=password, client_secret=client_secret,
        store_refresh_token=True
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == [
        mock.call.set_refresh_token(client_id=client_id, issuer=issuer, refresh_token=oidc_mock.state["refresh_token"])
    ]


def test_authenticate_oidc_resource_owner_password_credentials_client_from_config(requests_mock, auth_config):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    username, password = "john", "j0hn"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="password",
        expected_client_id=client_id,
        expected_fields={
            "username": username, "password": password, "scope": "openid", "client_secret": client_secret
        },
        oidc_issuer=issuer,
    )
    auth_config.set_oidc_client_config(
        backend=API_URL, provider_id="oi", client_id=client_id, client_secret=client_secret
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_resource_owner_password_credentials(username=username, password=password)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.parametrize(
    ["store_refresh_token", "scopes_supported", "expected_scopes"],
    [
        (False, ["openid", "email"], "openid"),
        (False, ["openid", "email", "offline_access"], "openid"),
        (True, ["openid", "email"], "openid"),
        (True, ["openid", "email", "offline_access"], "offline_access openid"),
    ]
)
def test_authenticate_oidc_device_flow_with_secret(
    requests_mock, store_refresh_token, scopes_supported, expected_scopes, oidc_device_code_flow_checker
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        expected_fields={
            "scope": expected_scopes or "openid",
            "client_secret": client_secret
        },
        scopes_supported=scopes_supported or ["openid"],
        oidc_issuer=issuer,
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    # with oidc_mock
    with oidc_device_code_flow_checker():
        conn.authenticate_oidc_device(
            client_id=client_id, client_secret=client_secret, store_refresh_token=store_refresh_token
        )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    if store_refresh_token:
        refresh_token = oidc_mock.state["refresh_token"]
        assert refresh_token_store.mock_calls == [
            mock.call.set_refresh_token(client_id=client_id, issuer=issuer, refresh_token=refresh_token)
        ]
    else:
        assert refresh_token_store.mock_calls == []


def test_authenticate_oidc_device_flow_with_secret_from_config(
    requests_mock, auth_config, caplog, oidc_device_code_flow_checker
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        expected_fields={
            "scope": "openid", "client_secret": client_secret
        },
        scopes_supported=["openid"],
        oidc_issuer=issuer,
    )
    auth_config.set_oidc_client_config(
        backend=API_URL, provider_id="oi", client_id=client_id, client_secret=client_secret
    )

    # With all this set up, kick off the openid connect flow
    caplog.set_level(logging.INFO)
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker():
        conn.authenticate_oidc_device()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []
    assert "No OIDC provider given, but only one available: 'oi'. Using that one." in caplog.text
    assert "Using client_id 'myclient' from config (provider 'oi')" in caplog.text


@pytest.mark.slow
def test_authenticate_oidc_device_flow_no_support(requests_mock, auth_config):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_issuer=issuer,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid"},
        scopes_supported=["openid"],
        device_code_flow_support=False
    )
    auth_config.set_oidc_client_config(
        backend=API_URL, provider_id="oi", client_id=client_id, client_secret=client_secret
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    with pytest.raises(OidcException, match="No support for device authorization grant"):
        conn.authenticate_oidc_device()


@pytest.mark.parametrize(["use_pkce", "expect_pkce"], [
    (None, False),
    (True, True),
    (False, False),
])
def test_authenticate_oidc_device_flow_pkce_multiple_providers_no_given(
    requests_mock, auth_config, caplog, use_pkce, expect_pkce, oidc_device_code_flow_checker
):
    """OIDC device flow + PKCE with multiple OIDC providers and none specified to use."""
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo Auth", "scopes": ["openid", "w"]},
            {"id": "bauth", "issuer": "https://bauth.test", "title": "Bar Auth", "scopes": ["openid", "w"]},
        ]
    })
    oidc_issuer = oidc_issuer = "https://fauth.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        expected_fields={
            "scope": "openid w",
            "code_verifier": True if expect_pkce else ABSENT,
            "code_challenge": True if expect_pkce else ABSENT,
        },
        scopes_supported=["openid", "w"],
        oidc_issuer=oidc_issuer,
    )
    assert auth_config.load() == {}

    # With all this set up, kick off the openid connect flow
    caplog.set_level(logging.INFO)
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker(url=f"{oidc_issuer}/dc"):
        conn.authenticate_oidc_device(client_id=client_id, use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/fauth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []
    assert "No OIDC provider given. Using first provider 'fauth' as advertised by backend." in caplog.text


@pytest.mark.parametrize(["use_pkce", "expect_pkce"], [
    (None, False),
    (True, True),
    (False, False),
])
def test_authenticate_oidc_device_flow_pkce_multiple_provider_one_config_no_given(
    requests_mock, auth_config, caplog, use_pkce, expect_pkce, oidc_device_code_flow_checker
):
    """OIDC device flow + PKCE with multiple OIDC providers, one in config and none specified to use."""
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo", "scopes": ["openid"]},
            {"id": "bauth", "issuer": "https://bauth.test", "title": "Bar", "scopes": ["openid"]},
        ]
    })
    oidc_issuer = "https://fauth.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        expected_fields={
            "scope": "openid",
            "code_verifier": True if expect_pkce else ABSENT,
            "code_challenge": True if expect_pkce else ABSENT,
        },
        scopes_supported=["openid"],
        oidc_issuer=oidc_issuer,
    )
    assert auth_config.load() == {}
    auth_config.set_oidc_client_config(backend=API_URL, provider_id="fauth", client_id=client_id)

    # With all this set up, kick off the openid connect flow
    caplog.set_level(logging.INFO)
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker(url=f"{oidc_issuer}/dc"):
        conn.authenticate_oidc_device(use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/fauth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []
    assert "No OIDC provider given, but only one in config (for backend 'https://oeo.test/'): 'fauth'. Using that one." in caplog.text
    assert "Using client_id 'myclient' from config (provider 'fauth')" in caplog.text


def test_authenticate_oidc_device_flow_pkce_multiple_provider_one_config_no_given_default_client(
    requests_mock, auth_config, oidc_device_code_flow_checker
):
    """
    OIDC device flow + default_clients + PKCE with multiple OIDC providers, one in config and none specified to use.
    """
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    default_client_id = "dadefaultklient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo", "scopes": ["openid"]},
            {
                "id": "bauth", "issuer": "https://bauth.test", "title": "Bar", "scopes": ["openid"],
                "default_clients": [{
                    "id": default_client_id,
                    "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
                }]
            },
        ]
    })
    oidc_issuer = "https://bauth.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=default_client_id,
        expected_fields={
            "scope": "openid", "code_verifier": True, "code_challenge": True
        },
        scopes_supported=["openid"],
        oidc_issuer=oidc_issuer,
    )
    assert auth_config.load() == {}
    auth_config.set_oidc_client_config(backend=API_URL, provider_id="bauth", client_id=None)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker(url=f"{oidc_issuer}/dc"):
        conn.authenticate_oidc_device()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/bauth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.parametrize(
    ["provider_id_arg", "provider_id_env", "provider_id_conf", "expected_provider"],
    [
        (None, None, None, "oidc1"),
        ("oidc2", None, None, "oidc2"),
        (None, "oidc2", None, "oidc2"),
        (None, None, "oidc2", "oidc2"),
        ("oidc2", "oidc3", "oidc4", "oidc2"),
        ("oidc2", None, "oidc4", "oidc2"),
        (None, "oidc3", "oidc4", "oidc3"),
    ],
)
def test_authenticate_oidc_device_flow_pkce_multiple_provider_resolution(
    requests_mock,
    auth_config,
    oidc_device_code_flow_checker,
    provider_id_arg,
    provider_id_env,
    provider_id_conf,
    expected_provider,
    monkeypatch,
):
    """
    OIDC device flow + default_clients + PKCE with multiple OIDC providers: provider resolution/precedence
    """
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "klientid"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": f"oidc{p}",
                    "issuer": f"https://oidc{p}.test",
                    "title": "One",
                    "scopes": ["openid"],
                    "default_clients": [
                        {
                            "id": client_id,
                            "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"],
                        }
                    ],
                }
                for p in [1, 2, 3, 4]
            ]
        },
    )
    oidc_issuer = f"https://{expected_provider}.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid", "code_verifier": True, "code_challenge": True},
        scopes_supported=["openid"],
        oidc_issuer=oidc_issuer,
    )
    if provider_id_env:
        monkeypatch.setenv("OPENEO_AUTH_PROVIDER_ID", provider_id_env)
    assert auth_config.load() == {}
    if provider_id_conf:
        auth_config.set_oidc_client_config(backend=API_URL, provider_id=provider_id_conf, client_id=None)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker(url=f"{oidc_issuer}/dc"):
        conn.authenticate_oidc_device(client_id=client_id, provider_id=provider_id_arg)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == f"oidc/{expected_provider}/" + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.parametrize(
    ["grant_types", "use_pkce", "expect_pkce"],
    [
        (["urn:ietf:params:oauth:grant-type:device_code+pkce"], True, True),
        (["urn:ietf:params:oauth:grant-type:device_code+pkce"], None, True),
        (["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"], None, True),
        (["urn:ietf:params:oauth:grant-type:device_code"], None, False),
        (["urn:ietf:params:oauth:grant-type:device_code"], False, False),
    ],
)
def test_authenticate_oidc_device_flow_pkce_default_client_handling(
    requests_mock, grant_types, use_pkce, expect_pkce, oidc_device_code_flow_checker
):
    """
    OIDC device authn grant + secret/PKCE/neither: default client grant_types handling
    """
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    default_client_id = "dadefaultklient"
    oidc_issuer = "https://auth.test"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": "auth",
                    "issuer": oidc_issuer,
                    "title": "Auth",
                    "scopes": ["openid"],
                    "default_clients": [
                        {"id": default_client_id, "grant_types": grant_types}
                    ],
                },
            ]
        },
    )

    expected_fields = {
        "scope": "openid",
    }
    if expect_pkce:
        expected_fields["code_verifier"] = True
        expected_fields["code_challenge"] = True
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=default_client_id,
        expected_fields=expected_fields,
        scopes_supported=["openid"],
        oidc_issuer=oidc_issuer,
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker(url=f"{oidc_issuer}/dc"):
        conn.authenticate_oidc_device(use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/auth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


def test_authenticate_oidc_device_flow_pkce_store_refresh_token(requests_mock, oidc_device_code_flow_checker):
    """OIDC device authn grant + PKCE + refresh token storage"""
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    default_client_id = "dadefaultklient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {
                "id": "auth", "issuer": "https://auth.test", "title": "Auth", "scopes": ["openid"],
                "default_clients": [{
                    "id": default_client_id,
                    "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
                }]
            },
        ]
    })

    expected_fields = {
        "scope": "openid", "code_verifier": True, "code_challenge": True
    }
    oidc_issuer = "https://auth.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=default_client_id,
        expected_fields=expected_fields,
        scopes_supported=["openid"],
        oidc_issuer=oidc_issuer,
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker(url=f"{oidc_issuer}/dc"):
        conn.authenticate_oidc_device(store_refresh_token=True)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/auth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == [
        mock.call.set_refresh_token(
            client_id=default_client_id, issuer="https://auth.test",
            refresh_token=oidc_mock.state["refresh_token"]
        )
    ]


def test_authenticate_oidc_refresh_token(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    refresh_token = "r3fr35h!"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=issuer,
        expected_fields={"refresh_token": refresh_token}
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_refresh_token(refresh_token=refresh_token, client_id=client_id)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]


def test_authenticate_oidc_refresh_token_expired(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=issuer,
        expected_fields={"refresh_token": "c0rr3ct.t0k3n"}
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    with pytest.raises(OidcException, match="Failed to retrieve access token.*invalid refresh token"):
        conn.authenticate_oidc_refresh_token(refresh_token="wr0n8.t0k3n!", client_id=client_id)
    assert isinstance(conn.auth, NullAuth)


@pytest.mark.parametrize(
    ["provider_id_arg", "provider_id_env", "provider_id_conf", "expected_provider"],
    [
        (None, None, None, "oidc1"),
        ("oidc2", None, None, "oidc2"),
        (None, "oidc2", None, "oidc2"),
        (None, None, "oidc2", "oidc2"),
        ("oidc2", "oidc3", "oidc4", "oidc2"),
        ("oidc2", None, "oidc4", "oidc2"),
        (None, "oidc3", "oidc4", "oidc3"),
    ],
)
def test_authenticate_oidc_refresh_token_multiple_provider_resolution(
    requests_mock, auth_config, provider_id_arg, provider_id_env, provider_id_conf, expected_provider, monkeypatch
):
    """Multiple OIDC Providers: provider resolution/precedence"""
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    refresh_token = "r3fr35h!"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": f"oidc{p}",
                    "issuer": f"https://oidc{p}.test",
                    "title": "One",
                    "scopes": ["openid"],
                    "default_clients": [
                        {
                            "id": client_id,
                            "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"],
                        }
                    ],
                }
                for p in [1, 2, 3, 4]
            ]
        },
    )
    oidc_issuer = f"https://{expected_provider}.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"refresh_token": refresh_token},
    )
    if provider_id_env:
        monkeypatch.setenv("OPENEO_AUTH_PROVIDER_ID", provider_id_env)
    assert auth_config.load() == {}
    if provider_id_conf:
        auth_config.set_oidc_client_config(backend=API_URL, provider_id=provider_id_conf, client_id=None)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_refresh_token(refresh_token=refresh_token, client_id=client_id, provider_id=provider_id_arg)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == f"oidc/{expected_provider}/" + oidc_mock.state["access_token"]


@pytest.mark.parametrize("store_refresh_token", [True, False])
def test_authenticate_oidc_auto_with_existing_refresh_token(requests_mock, refresh_token_store, store_refresh_token):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    orig_refresh_token = "r3fr35h!"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=issuer,
        expected_fields={"refresh_token": orig_refresh_token}
    )
    refresh_token_store.set_refresh_token(issuer=issuer, client_id=client_id, refresh_token=orig_refresh_token)

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc(client_id=client_id, store_refresh_token=store_refresh_token)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]

    new_refresh_token = refresh_token_store.get_refresh_token(issuer=issuer, client_id=client_id)
    assert new_refresh_token == orig_refresh_token
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == ["refresh_token"]


@pytest.mark.parametrize(
    ["use_pkce", "expect_pkce"],
    [
        (None, False),
        (True, True),
        (False, False),
    ],
)
def test_authenticate_oidc_auto_no_existing_refresh_token(
    requests_mock, refresh_token_store, use_pkce, expect_pkce, oidc_device_code_flow_checker
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    # TODO: we're mixing both refresh_token and device_code flow in same mock here
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_client_id=client_id,
        expected_grant_type=None,
        oidc_issuer=issuer,
        expected_fields={
            "refresh_token": "unkn0wn",
            "scope": "openid",
            "code_verifier": True if expect_pkce else ABSENT,
            "code_challenge": True if expect_pkce else ABSENT,
        }
    )

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker():
        conn.authenticate_oidc(client_id=client_id, use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == [
        "urn:ietf:params:oauth:grant-type:device_code"
    ]


@pytest.mark.parametrize(
    ["use_pkce", "expect_pkce"],
    [
        (None, False),
        (True, True),
        (False, False),
    ],
)
def test_authenticate_oidc_auto_expired_refresh_token(
    requests_mock, refresh_token_store, use_pkce, expect_pkce, oidc_device_code_flow_checker
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    issuer = "https://oidc.test"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    # TODO: we're mixing both refresh_token and device_code flow in same mock here
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_client_id=client_id,
        expected_grant_type=None,
        oidc_issuer=issuer,
        expected_fields={
            "refresh_token": "unkn0wn",
            "scope": "openid",
            "code_verifier": True if expect_pkce else ABSENT,
            "code_challenge": True if expect_pkce else ABSENT,
        }
    )
    refresh_token_store.set_refresh_token(issuer=issuer, client_id=client_id, refresh_token="0ld.t0k3n")

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker():
        conn.authenticate_oidc(client_id=client_id, use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == [
        "refresh_token",
        "urn:ietf:params:oauth:grant-type:device_code",
    ]
    assert oidc_mock.grant_request_history[0]["response"] == {"error": "invalid refresh token"}


@pytest.mark.parametrize(
    ["env_provider_id", "expected_provider_id"],
    [
        (None, "oi"),
        ("oi", "oi"),
        ("dc", "dc"),
    ],
)
def test_authenticate_oidc_method_client_credentials_from_env(
    requests_mock, monkeypatch, env_provider_id, expected_provider_id
):
    client_id = "myclient"
    client_secret = "$3cr3t!"
    monkeypatch.setenv("OPENEO_AUTH_METHOD", "client_credentials")
    monkeypatch.setenv("OPENEO_AUTH_CLIENT_ID", client_id)
    monkeypatch.setenv("OPENEO_AUTH_CLIENT_SECRET", client_secret)
    if env_provider_id:
        monkeypatch.setenv("OPENEO_AUTH_PROVIDER_ID", env_provider_id)
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {"id": "oi", "issuer": "https://oi.test", "title": "example", "scopes": ["openid"]},
                {"id": "dc", "issuer": "https://dc.test", "title": "example", "scopes": ["openid"]},
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        oidc_issuer=f"https://{expected_provider_id}.test",
        expected_fields={"scope": "openid", "client_secret": client_secret},
    )

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == f"oidc/{expected_provider_id}/" + oidc_mock.state["access_token"]


def _setup_get_me_handler(requests_mock, oidc_mock: OidcMock, token_invalid_status_code: int = 403):
    def get_me(request: requests.Request, context):
        """handler for `GET /me` (with access_token checking)"""
        auth_header = request.headers["Authorization"]
        oidc_provider, access_token = re.match(r"Bearer oidc/(?P<p>\w+)/(?P<a>.*)", auth_header).group("p", "a")
        try:
            user_id = oidc_mock.validate_access_token(access_token)["user_id"]
        except LookupError:
            context.status_code = token_invalid_status_code
            return {"code": "TokenInvalid", "message": "Authorization token has expired or is invalid."}

        return {
            "user_id": user_id,
            # Normally not part of `GET /me` response, but just for test/inspection purposes
            "_used_oidc_provider": oidc_provider,
            "_used_access_token": access_token,
        }

    requests_mock.get(API_URL + "me", json=get_me)


@pytest.mark.parametrize(
    ["invalidate", "token_invalid_status_code"],
    [
        (False, 403),
        (True, 403),
        (True, 401),
    ],
)
def test_authenticate_oidc_auto_renew_expired_access_token_initial_refresh_token(
    requests_mock, refresh_token_store, invalidate, token_invalid_status_code, caplog
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    initial_refresh_token = "r3fr35h!"
    oidc_issuer = "https://oidc.test"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": "oi",
                    "issuer": oidc_issuer,
                    "title": "example",
                    "scopes": ["openid"],
                }
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"refresh_token": initial_refresh_token}
    )
    _setup_get_me_handler(
        requests_mock=requests_mock, oidc_mock=oidc_mock, token_invalid_status_code=token_invalid_status_code
    )
    caplog.set_level(logging.INFO)

    # Explicit authentication with `authenticate_oidc_refresh_token`
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_refresh_token(
        refresh_token=initial_refresh_token, client_id=client_id, store_refresh_token=True
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    # Just one "refresh_token" auth request so far
    assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["refresh_token"]
    access_token1 = oidc_mock.state["access_token"]
    assert "refresh_token" not in oidc_mock.state
    # Do request that requires auth headers
    assert conn.describe_account() == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token1,
    }

    # Expire access token and expect new refresh_token auth request with latest refresh token
    if invalidate:
        oidc_mock.invalidate_access_token()
    # Do request that requires auth headers and might trigger re-authentication
    assert "403 TokenInvalid" not in caplog.text
    get_me_response = conn.describe_account()
    access_token2 = oidc_mock.state["access_token"]
    assert "refresh_token" not in oidc_mock.state
    if invalidate:
        # Two "refresh_token" auth requests should have happened now
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["refresh_token", "refresh_token"]
        assert access_token2 != access_token1
        assert f"OIDC access token expired ({token_invalid_status_code} TokenInvalid)" in caplog.text
        assert "Obtained new access token (grant 'refresh_token')" in caplog.text
    else:
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["refresh_token"]
        assert access_token2 == access_token1
        assert "TokenInvalid" not in caplog.text

    assert get_me_response == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token2,
    }


@pytest.mark.parametrize(
    ["invalidate", "token_invalid_status_code"],
    [
        (False, 403),
        (True, 403),
        (True, 401),
    ],
)
def test_authenticate_oidc_auto_renew_expired_access_token_initial_device_code(
    requests_mock, refresh_token_store, invalidate, token_invalid_status_code, caplog, oidc_device_code_flow_checker
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    oidc_issuer = "https://oidc.test"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": "oi",
                    "issuer": oidc_issuer,
                    "title": "example",
                    "scopes": ["openid"],
                }
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={
            "scope": "openid",
            "code_verifier": True,
            "code_challenge": True,
        },
        scopes_supported=["openid"],
    )
    _setup_get_me_handler(
        requests_mock=requests_mock, oidc_mock=oidc_mock, token_invalid_status_code=token_invalid_status_code
    )
    caplog.set_level(logging.INFO)

    # Explicit authentication with `authenticate_oidc_device`
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker():
        conn.authenticate_oidc_device(client_id=client_id, use_pkce=True, store_refresh_token=True)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    # Just one "refresh_token" auth request so far
    assert [h["grant_type"] for h in oidc_mock.grant_request_history] == [
        "urn:ietf:params:oauth:grant-type:device_code"
    ]
    access_token1 = oidc_mock.state["access_token"]
    refresh_token1 = oidc_mock.state["refresh_token"]
    assert refresh_token1 is not None
    # Do request that requires auth headers
    assert conn.describe_account() == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token1,
    }

    # Expire access token and expect new refresh_token auth request with latest refresh token
    if invalidate:
        oidc_mock.invalidate_access_token()
        oidc_mock.expected_fields["refresh_token"] = refresh_token1
        oidc_mock.expected_grant_type = "refresh_token"
    # Do request that requires auth headers and might trigger re-authentication
    assert "403 TokenInvalid" not in caplog.text
    get_me_response = conn.describe_account()
    access_token2 = oidc_mock.state["access_token"]
    refresh_token2 = oidc_mock.state["refresh_token"]
    if invalidate:
        # Two "refresh_token" auth requests should have happened now
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == [
            "urn:ietf:params:oauth:grant-type:device_code",
            "refresh_token",
        ]
        assert (access_token2, refresh_token2) != (access_token1, refresh_token1)
        assert f"OIDC access token expired ({token_invalid_status_code} TokenInvalid)" in caplog.text
        assert "Obtained new access token (grant 'refresh_token')" in caplog.text
    else:
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == [
            "urn:ietf:params:oauth:grant-type:device_code"
        ]
        assert (access_token2, refresh_token2) == (access_token1, refresh_token1)
        assert "403 TokenInvalid" not in caplog.text

    assert get_me_response == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token2,
    }


@pytest.mark.parametrize(
    ["token_invalid_status_code"],
    [
        (403,),
        (401,),
    ],
)
def test_authenticate_oidc_auto_renew_expired_access_token_invalid_refresh_token(
    requests_mock, refresh_token_store, caplog, oidc_device_code_flow_checker, token_invalid_status_code
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    oidc_issuer = "https://oidc.test"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": "oi",
                    "issuer": oidc_issuer,
                    "title": "example",
                    "scopes": ["openid"],
                }
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={
            "scope": "openid",
            "code_verifier": True,
            "code_challenge": True,
        },
        scopes_supported=["openid"],
    )
    _setup_get_me_handler(
        requests_mock=requests_mock, oidc_mock=oidc_mock, token_invalid_status_code=token_invalid_status_code
    )
    caplog.set_level(logging.INFO)

    # Explicit authentication with `authenticate_oidc_device`
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with oidc_device_code_flow_checker():
        conn.authenticate_oidc_device(client_id=client_id, use_pkce=True, store_refresh_token=True)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    # Just one "refresh_token" auth request so far
    assert [h["grant_type"] for h in oidc_mock.grant_request_history] == [
        "urn:ietf:params:oauth:grant-type:device_code"
    ]
    access_token1 = oidc_mock.state["access_token"]
    refresh_token1 = oidc_mock.state["refresh_token"]
    assert refresh_token1 is not None
    # Do request that requires auth headers
    assert conn.describe_account() == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token1,
    }

    # Expire access token and refresh token too
    oidc_mock.invalidate_access_token()
    oidc_mock.expected_fields["refresh_token"] = "sorry-not-accepting-refresh-tokens-now"
    oidc_mock.expected_grant_type = "refresh_token"
    # Do request that requires auth headers triggers attempt to re-authenticate (which will fail)
    assert "TokenInvalid" not in caplog.text
    with pytest.raises(
        OpenEoApiError,
        match=re.escape(f"[{token_invalid_status_code}] TokenInvalid: Authorization token has expired or is invalid."),
    ):
        conn.describe_account()

    assert f"OIDC access token expired ({token_invalid_status_code} TokenInvalid)" in caplog.text
    assert "Failed to obtain new access token (grant 'refresh_token')" in caplog.text


def test_authenticate_oidc_auto_renew_expired_access_token_other_errors(requests_mock, caplog):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    initial_refresh_token = "r3fr35h!"
    oidc_issuer = "https://oidc.test"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={
            "providers": [
                {
                    "id": "oi",
                    "issuer": oidc_issuer,
                    "title": "example",
                    "scopes": ["openid"],
                }
            ]
        },
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"refresh_token": initial_refresh_token}
    )

    def get_me(request: requests.Request, context):
        """handler for `GET /me` (with access_token checking)"""
        context.status_code = 500
        return {"code": "Internal", "message": "Something's not right."}

    requests_mock.get(API_URL + "me", json=get_me)

    caplog.set_level(logging.DEBUG)

    # Explicit authentication with `authenticate_oidc_refresh_token`
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_refresh_token(refresh_token=initial_refresh_token, client_id=client_id)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]

    # Do request that will fail with "Internal" error
    with pytest.raises(OpenEoApiError, match=re.escape("[500] Internal: Something's not right.")):
        conn.describe_account()
    # The re-authentication retry logic should not be triggered
    assert "new access token" not in caplog.text


@pytest.mark.parametrize(
    ["invalidate", "token_invalid_status_code"],
    [
        (False, 403),
        (True, 403),
        (True, 401),
    ],
)
def test_authenticate_oidc_auto_renew_expired_access_token_initial_client_credentials(
    requests_mock, refresh_token_store, invalidate, token_invalid_status_code, caplog
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={"providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]},
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
        oidc_issuer=issuer,
    )

    _setup_get_me_handler(
        requests_mock=requests_mock, oidc_mock=oidc_mock, token_invalid_status_code=token_invalid_status_code
    )
    caplog.set_level(logging.INFO)

    # Explicit authentication with `authenticate_oidc_refresh_token`
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_client_credentials(client_id=client_id, client_secret=client_secret)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == "oidc/oi/" + oidc_mock.state["access_token"]
    # Just one "client_credentials" auth request so far
    assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["client_credentials"]
    access_token1 = oidc_mock.state["access_token"]
    assert "refresh_token" not in oidc_mock.state
    # Do request that requires auth headers
    assert conn.describe_account() == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token1,
    }

    # Expire access token and expect new client_credentials auth request
    if invalidate:
        oidc_mock.invalidate_access_token()
    # Do request that requires auth headers and might trigger re-authentication
    assert "403 TokenInvalid" not in caplog.text
    get_me_response = conn.describe_account()
    access_token2 = oidc_mock.state["access_token"]
    assert "refresh_token" not in oidc_mock.state
    if invalidate:
        # Two "client_credentials" auth requests should have happened now
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == [
            "client_credentials",
            "client_credentials",
        ]
        assert access_token2 != access_token1
        assert f"OIDC access token expired ({token_invalid_status_code} TokenInvalid)" in caplog.text
        assert "Obtained new access token (grant 'client_credentials')" in caplog.text
    else:
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["client_credentials"]
        assert access_token2 == access_token1
        assert "TokenInvalid" not in caplog.text

    assert get_me_response == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token2,
    }


@pytest.mark.parametrize(
    ["token_invalid_status_code"],
    [
        (403,),
        (401,),
    ],
)
def test_authenticate_oidc_auto_renew_expired_access_token_initial_client_credentials_blocked(
    requests_mock, refresh_token_store, caplog, token_invalid_status_code
):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    requests_mock.get(
        API_URL + "credentials/oidc",
        json={"providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]},
    )
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
        oidc_issuer=issuer,
    )

    _setup_get_me_handler(
        requests_mock=requests_mock, oidc_mock=oidc_mock, token_invalid_status_code=token_invalid_status_code
    )
    caplog.set_level(logging.INFO)

    # Explicit authentication with `authenticate_oidc_refresh_token`
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_client_credentials(client_id=client_id, client_secret=client_secret)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == "oidc/oi/" + oidc_mock.state["access_token"]
    # Just one "client_credentials" auth request so far
    assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["client_credentials"]
    access_token1 = oidc_mock.state["access_token"]
    assert "refresh_token" not in oidc_mock.state
    # Do request that requires auth headers
    assert conn.describe_account() == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token1,
    }

    # Expire access token don't accept client credentials anymore
    oidc_mock.invalidate_access_token()
    requests_mock.post(oidc_mock.token_endpoint, status_code=401, text="nope")
    # Do request that requires auth headers and might trigger re-authentication
    assert f"{token_invalid_status_code} TokenInvalid" not in caplog.text
    with pytest.raises(
        OpenEoApiError,
        match=re.escape(f"[{token_invalid_status_code}] TokenInvalid: Authorization token has expired or is invalid."),
    ):
        conn.describe_account()

    assert f"OIDC access token expired ({token_invalid_status_code} TokenInvalid)" in caplog.text
    assert "Failed to obtain new access token (grant 'client_credentials')" in caplog.text


class TestAuthenticateOidcAccessToken:
    @pytest.fixture(autouse=True)
    def _setup(self, requests_mock):
        requests_mock.get(API_URL, json=build_capabilities())
        requests_mock.get(
            API_URL + "credentials/oidc",
            json={"providers": [{"id": "oi", "issuer": "https://oidc.test", "title": "example", "scopes": ["openid"]}]},
        )

    def test_authenticate_oidc_access_token_default_provider(self):
        connection = Connection(API_URL)
        connection.authenticate_oidc_access_token(access_token="Th3Tok3n!@#")
        assert isinstance(connection.auth, BearerAuth)
        assert connection.auth.bearer == "oidc/oi/Th3Tok3n!@#"

    def test_authenticate_oidc_access_token_with_provider(self):
        connection = Connection(API_URL)
        connection.authenticate_oidc_access_token(access_token="Th3Tok3n!@#", provider_id="oi")
        assert isinstance(connection.auth, BearerAuth)
        assert connection.auth.bearer == "oidc/oi/Th3Tok3n!@#"

    def test_authenticate_oidc_access_token_wrong_provider(self):
        connection = Connection(API_URL)
        with pytest.raises(
            OpenEoClientException,
            match=re.escape("Requested OIDC provider 'nope' not available. Should be one of ['oi']."),
        ):
            connection.authenticate_oidc_access_token(access_token="Th3Tok3n!@#", provider_id="nope")


def test_load_collection_arguments_100(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    requests_mock.get(API_URL + "collections/FOO", json={
        "summaries": {"eo:bands": [{"name": "red"}, {"name": "green"}, {"name": "blue"}]}
    })
    spatial_extent = {"west": 1, "south": 2, "east": 3, "north": 4}
    temporal_extent = ["2019-01-01", "2019-01-22"]
    im = conn.load_collection(
        "FOO", spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=["red", "green"]
    )
    assert im._pg.process_id == "load_collection"
    assert im._pg.arguments == {
        "id": "FOO",
        "spatial_extent": spatial_extent,
        "temporal_extent": temporal_extent,
        "bands": ["red", "green"]
    }


def test_load_result(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    con = Connection(API_URL)
    cube = con.load_result("j0bi6")
    assert cube.flat_graph() == {
        "loadresult1": {"process_id": "load_result", "arguments": {"id": "j0bi6"}, "result": True}
    }


def test_load_result_filters(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    con = Connection(API_URL)
    cube = con.load_result(
        "j0bi6",
        spatial_extent={"west": 3, "south": 4, "east": 5, "north": 6},
        temporal_extent=["2021-10-01", "2021-12-12"],
        bands=["red"],
    )
    assert cube.flat_graph() == {
        "loadresult1": {
            "process_id": "load_result",
            "arguments": {
                "id": "j0bi6",
                "spatial_extent": {"west": 3, "south": 4, "east": 5, "north": 6},
                "temporal_extent": ["2021-10-01", "2021-12-12"],
                "bands": ["red"],
            },
            "result": True,
        }
    }


class TestLoadStac:
    def test_basic(self, con120):
        cube = con120.load_stac("https://provider.test/dataset")
        assert cube.flat_graph() == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": "https://provider.test/dataset"},
                "result": True,
            }
        }

    def test_extents(self, con120):
        cube = con120.load_stac(
            "https://provider.test/dataset",
            spatial_extent={"west": 1, "south": 2, "east": 3, "north": 4},
            temporal_extent=["2023-05-10", "2023-06-01"],
            bands=["B02", "B03"],
        )
        assert cube.flat_graph() == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {
                    "url": "https://provider.test/dataset",
                    "spatial_extent": {"east": 3, "north": 4, "south": 2, "west": 1},
                    "temporal_extent": ["2023-05-10", "2023-06-01"],
                    "bands": ["B02", "B03"],
                },
                "result": True,
            }
        }

    def test_properties(self, con120):
        cube = con120.load_stac("https://provider.test/dataset", properties={"platform": lambda p: p == "S2A"})
        assert cube.flat_graph() == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {
                    "url": "https://provider.test/dataset",
                    "properties": {
                        "platform": {
                            "process_graph": {
                                "eq1": {
                                    "arguments": {"x": {"from_parameter": "value"}, "y": "S2A"},
                                    "process_id": "eq",
                                    "result": True,
                                }
                            }
                        }
                    },
                },
                "result": True,
            }
        }

    def test_load_stac_from_job_canonical(self, con120, requests_mock):
        requests_mock.get(
            API_URL + "jobs/j0bi6/results",
            json={
                "links": [
                    {
                        "href": "https://wrong.test",
                        "rel": "self",
                    },
                    {
                        "href": "https://stac.test",
                        "rel": "canonical",
                    },
                ]
            },
        )
        job = con120.job("j0bi6")
        cube = con120.load_stac_from_job(job)

        fg = cube.flat_graph()
        assert fg == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": "https://stac.test"},
                "result": True,
            }
        }

    def test_load_stac_from_job_unsigned(self, con120, requests_mock):
        requests_mock.get(
            API_URL + "jobs/j0bi6/results",
            json={
                "links": [
                    {
                        "href": "https://wrong.test",
                        "rel": "self",
                    },
                ]
            },
        )
        job = con120.job("j0bi6")
        unsigned_link = job.get_results_metadata_url(full=True)

        cube = con120.load_stac_from_job(job)
        fg = cube.flat_graph()

        assert fg == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": API_URL + "jobs/j0bi6/results"},
                "result": True,
            }
        }

    def test_load_stac_from_job_from_jobid(self, con120, requests_mock):
        requests_mock.get(
            API_URL + "jobs/j0bi6/results",
            json={
                "links": [
                    {
                        "href": "https://wrong.test",
                        "rel": "self",
                    },
                    {
                        "href": "https://stac.test",
                        "rel": "canonical",
                    },
                ]
            },
        )
        jobid = "j0bi6"
        cube = con120.load_stac_from_job(jobid)

        fg = cube.flat_graph()
        assert fg == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": "https://stac.test"},
                "result": True,
            }
        }

    def test_load_stac_from_job_empty_result(self, con120, requests_mock):
        requests_mock.get(
            API_URL + "jobs/j0bi6/results",
            json={"links": []},
        )
        jobid = "j0bi6"
        cube = con120.load_stac_from_job(jobid)

        fg = cube.flat_graph()
        assert fg == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": API_URL + "jobs/j0bi6/results"},
                "result": True,
            }
        }

    @pytest.mark.parametrize("temporal_dim", ["t", "datezz"])
    def test_load_stac_reduce_temporal(self, con120, tmp_path, temporal_dim):
        # TODO: reusable utility to create/generate a STAC resource for testing
        #       (a file, but preferably a URL, but that requires urllib mocking)
        stac_path = tmp_path / "stac.json"
        stac_data = StacDummyBuilder.collection(
            cube_dimensions={temporal_dim: {"type": "temporal", "extent": ["2024-01-01", "2024-04-04"]}}
        )
        stac_path.write_text(json.dumps(stac_data))

        cube = con120.load_stac(str(stac_path))
        reduced = cube.reduce_temporal("max")
        assert reduced.flat_graph() == {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": str(stac_path)},
            },
            "reducedimension1": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_node": "loadstac1"},
                    "dimension": temporal_dim,
                    "reducer": {
                        "process_graph": {
                            "max1": {
                                "arguments": {"data": {"from_parameter": "data"}},
                                "process_id": "max",
                                "result": True,
                            }
                        }
                    },
                },
                "result": True,
            },
        }


@pytest.mark.parametrize(
    "data",
    [
        {"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]},
        """{"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]}""",
        shapely.geometry.Polygon([[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]),
    ],
)
def test_load_geojson(con100, data, dummy_backend):
    vc = con100.load_geojson(data)
    assert isinstance(vc, VectorCube)
    vc.execute()
    assert dummy_backend.get_pg() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {
                "data": {"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]},
                "properties": [],
            },
            "result": True,
        }
    }


def test_load_url(con100, dummy_backend, requests_mock):
    file_formats = {
        "input": {"GeoJSON": {"gis_data_type": ["vector"]}},
    }
    requests_mock.get(API_URL + "file_formats", json=file_formats)
    vc = con100.load_url("https://example.com/geometry.json", format="GeoJSON")
    assert isinstance(vc, VectorCube)
    vc.execute()
    assert dummy_backend.get_pg() == {
        "loadurl1": {
            "process_id": "load_url",
            "arguments": {"url": "https://example.com/geometry.json", "format": "GeoJSON"},
            "result": True,
        }
    }


def test_list_file_formats(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    file_formats = {
        "input": {"GeoJSON": {"gis_data_type": ["vector"]}},
        "output": {"GTiff": {"gis_data_types": ["raster"]}},
    }
    m = requests_mock.get(API_URL + "file_formats", json=file_formats)
    assert conn.list_file_formats() == file_formats
    assert m.call_count == 1
    # Check caching of file formats
    assert conn.list_file_formats() == file_formats
    assert m.call_count == 1


def test_list_file_formats_error(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    m = requests_mock.get(API_URL + "file_formats", status_code=204)
    with pytest.raises(OpenEoRestError):
        conn.list_file_formats()
    assert m.call_count == 1
    # Error is not cached
    with pytest.raises(OpenEoRestError):
        conn.list_file_formats()
    assert m.call_count == 2


def test_list_service_types(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    service_types = {"WMS": {"configuration": {}, "process_parameters": []}}
    m = requests_mock.get(API_URL + "service_types", json=service_types)
    assert conn.list_service_types() == service_types
    assert m.call_count == 1
    # Check caching
    assert conn.list_service_types() == service_types
    assert m.call_count == 1


def test_list_service_types_error(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    m = requests_mock.get(API_URL + "service_types", status_code=204)
    with pytest.raises(OpenEoRestError):
        conn.list_service_types()
    assert m.call_count == 1
    # Error is not cached
    with pytest.raises(OpenEoRestError):
        conn.list_service_types()
    assert m.call_count == 2


def test_list_udf_runtimes(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    runtimes = {"Python": {"type": "language", "versions": {"3.12": {}}, "default": "3.12"}}
    m = requests_mock.get(API_URL + "udf_runtimes", json=runtimes)
    assert conn.list_udf_runtimes() == runtimes
    assert m.call_count == 1
    # Check caching
    assert conn.list_udf_runtimes() == runtimes
    assert m.call_count == 1


def test_list_udf_runtimes_error(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    m = requests_mock.get(API_URL + "udf_runtimes", status_code=204)
    with pytest.raises(OpenEoRestError):
        conn.list_udf_runtimes()
    assert m.call_count == 1
    # Error is not cached
    with pytest.raises(OpenEoRestError):
        conn.list_udf_runtimes()
    assert m.call_count == 2


def test_list_collections(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    collections = [
        {"id": "SENTINEL2", "description": "Sentinel 2 data"},
        {"id": "NDVI", "description": "NDVI data"},
    ]
    requests_mock.get(
        API_URL + "collections", json={"collections": collections, "links": []}
    )
    con = Connection(API_URL)
    assert con.list_collections() == collections


def test_describe_collection(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(
        API_URL + "collections/SENTINEL2",
        json={"id": "SENTINEL2", "description": "Sentinel 2 data"},
    )
    con = Connection(API_URL)
    assert con.describe_collection("SENTINEL2") == {
        "id": "SENTINEL2",
        "description": "Sentinel 2 data",
    }


def test_list_processes(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    processes = [{"id": "add"}, {"id": "mask"}]
    m = requests_mock.get(API_URL + "processes", json={"processes": processes})
    conn = Connection(API_URL)
    assert conn.list_processes() == processes
    assert m.call_count == 1
    # Check caching
    assert conn.list_processes() == processes
    assert m.call_count == 1


def test_list_processes_error(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    m = requests_mock.get(API_URL + "processes", status_code=204)
    with pytest.raises(OpenEoRestError):
        conn.list_processes()
    assert m.call_count == 1
    # Error is not cached
    with pytest.raises(OpenEoRestError):
        conn.list_processes()
    assert m.call_count == 2


def test_describe_processes(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    add = {"id": "add"}
    mask = {"id": "mask"}
    m = requests_mock.get(API_URL + "processes", json={"processes": [add, mask]})
    conn = Connection(API_URL)
    assert conn.describe_process("add") == add
    assert conn.describe_process("mask") == mask
    assert m.call_count == 1
    with pytest.raises(OpenEoClientException):
        conn.describe_process("invalid")


def test_list_processes_namespace(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    processes = [{"id": "add"}, {"id": "mask"}]
    m = requests_mock.get(API_URL + "processes/foo", json={"processes": processes})
    conn = Connection(API_URL)
    assert conn.list_processes(namespace="foo") == processes
    assert m.call_count == 1


def test_get_job(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)

    my_job = conn.job("the_job_id")
    assert my_job is not None


def test_default_timeout_default(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get("/foo", text=lambda req, ctx: repr(req.timeout))
    conn = connect(API_URL)
    assert conn.get("/foo").text == str(DEFAULT_TIMEOUT)
    assert conn.get("/foo", timeout=5).text == '5'


def test_default_timeout(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get("/foo", json=lambda req, ctx: repr(req.timeout))
    conn = connect(API_URL, default_timeout=2)
    assert conn.get("/foo").json() == '2'
    assert conn.get("/foo", timeout=5).json() == '5'


class DummyFlatGraphable(FlatGraphableMixin):
    def flat_graph(self) -> typing.Dict[str, dict]:
        return {"foo1": {"process_id": "foo"}}


@pytest.mark.parametrize(
    "pg",
    [
        {"foo1": {"process_id": "foo"}},
        {"process_graph": {"foo1": {"process_id": "foo"}}},
        DummyFlatGraphable(),
    ],
)
def test_download_100(requests_mock, pg):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    with mock.patch.object(conn, "request") as request:
        conn.download(pg)
    assert request.call_args_list == [
        mock.call(
            "post",
            path="/result",
            allow_redirects=False,
            stream=True,
            expected_status=200,
            json={"process": {"process_graph": {"foo1": {"process_id": "foo"}}}},
            timeout=DEFAULT_TIMEOUT_SYNCHRONOUS_EXECUTE,
        )
    ]


@pytest.mark.parametrize(
    "pg",
    [
        {"foo1": {"process_id": "foo"}},
        {"process_graph": {"foo1": {"process_id": "foo"}}},
        DummyFlatGraphable(),
    ],
)
def test_execute_100(requests_mock, pg):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    with mock.patch.object(conn, "request") as request:
        conn.execute(pg)
    assert request.call_args_list == [
        mock.call(
            "post",
            path="/result",
            allow_redirects=False,
            expected_status=200,
            json={"process": {"process_graph": {"foo1": {"process_id": "foo"}}}},
            timeout=DEFAULT_TIMEOUT_SYNCHRONOUS_EXECUTE,
        )
    ]


class TestUserDefinedProcesses:
    """Test for UDP features"""

    def test_create_udp(self, requests_mock, test_data):
        requests_mock.get(API_URL, json=build_capabilities(udp=True))
        requests_mock.get(API_URL + "processes", json={"processes": [{"id": "add"}]})
        conn = Connection(API_URL)

        new_udp = test_data.load_json("1.0.0/udp_details.json")

        def check_body(request):
            body = request.json()
            assert body["process_graph"] == new_udp["process_graph"]
            assert body["parameters"] == new_udp["parameters"]
            assert body["public"] is False
            return True

        adapter = requests_mock.put(API_URL + "process_graphs/evi", additional_matcher=check_body)

        conn.save_user_defined_process(
            user_defined_process_id="evi", process_graph=new_udp["process_graph"], parameters=new_udp["parameters"]
        )

        assert adapter.called

    def test_create_udp_public(self, requests_mock, test_data):
        requests_mock.get(API_URL, json=build_capabilities(udp=True))
        requests_mock.get(API_URL + "processes", json={"processes": [{"id": "add"}]})
        conn = Connection(API_URL)

        new_udp = test_data.load_json("1.0.0/udp_details.json")

        def check_body(request):
            body = request.json()
            assert body["process_graph"] == new_udp["process_graph"]
            assert body["parameters"] == new_udp["parameters"]
            assert body["public"] is True
            return True

        adapter = requests_mock.put(API_URL + "process_graphs/evi", additional_matcher=check_body)

        conn.save_user_defined_process(
            user_defined_process_id="evi",
            process_graph=new_udp["process_graph"],
            parameters=new_udp["parameters"],
            public=True,
        )

        assert adapter.called

    def test_create_udp_unsupported(self, requests_mock, test_data):
        requests_mock.get(API_URL, json=build_capabilities(udp=False))
        conn = Connection(API_URL)

        new_udp = test_data.load_json("1.0.0/udp_details.json")

        with pytest.raises(CapabilitiesException, match="Backend does not support user-defined processes."):
            _ = conn.save_user_defined_process(
                user_defined_process_id="evi", process_graph=new_udp["process_graph"], parameters=new_udp["parameters"]
            )

    def test_list_udps(self, requests_mock, test_data):
        requests_mock.get(API_URL, json=build_capabilities(udp=True))
        conn = Connection(API_URL)

        udp = test_data.load_json("1.0.0/udp_details.json")

        requests_mock.get(API_URL + "process_graphs", json={"processes": [udp]})

        user_udps = conn.list_user_defined_processes()

        assert len(user_udps) == 1
        assert user_udps[0] == udp


    def test_list_udps_unsupported(self, requests_mock):
        requests_mock.get(API_URL, json=build_capabilities(udp=False))
        conn = Connection(API_URL)
        with pytest.raises(CapabilitiesException, match="Backend does not support user-defined processes."):
            _ = conn.list_user_defined_processes()


    def test_get_udp(self, requests_mock):
        requests_mock.get(API_URL, json=build_capabilities(udp=True))
        conn = Connection(API_URL)

        udp = conn.user_defined_process("evi")

        assert udp.user_defined_process_id == "evi"

    def test_get_udp_unsupported(self, requests_mock):
        requests_mock.get(API_URL, json=build_capabilities(udp=False))
        conn = Connection(API_URL)
        with pytest.raises(CapabilitiesException, match="Backend does not support user-defined processes."):
            _ = conn.user_defined_process("evi")


def _gzip_compress(data: bytes) -> bytes:
    compressor = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
    return compressor.compress(data) + compressor.flush()


def _deflate_compress(data: bytes) -> bytes:
    compressor = zlib.compressobj()
    return compressor.compress(data) + compressor.flush()


@pytest.mark.parametrize(["content_encoding", "compress"], [
    (None, None),
    ("gzip", _gzip_compress),
    ("deflate", _deflate_compress),

])
def test_download_content_encoding(requests_mock, tmp_path, content_encoding, compress):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})

    tiff_data = b"hello world, I'm a tiff file."
    if content_encoding:
        response_data = compress(tiff_data)
        response_headers = {"Content-Encoding": content_encoding}
    else:
        response_data = tiff_data
        response_headers = {}
    requests_mock.post(API_URL + "result", content=response_data, headers=response_headers)

    conn = Connection(API_URL)
    output = tmp_path / "result.tiff"
    conn.download(graph={}, outputfile=output)
    with output.open("rb") as f:
        assert f.read() == tiff_data


def test_paginate_basic(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(
        API_URL + "result/1",
        json={"data": "first", "links": [{"rel": "next", "href": API_URL + "result-2"}]}
    )
    requests_mock.get(
        API_URL + "result-2",
        json={"data": "second", "links": [{"rel": "next", "href": API_URL + "resulthr33"}]}
    )
    requests_mock.get(
        API_URL + "resulthr33",
        json={"data": "third"}
    )
    con = Connection(API_URL)
    res = paginate(con, API_URL + "result/1")
    assert isinstance(res, typing.Iterator)
    assert list(res) == [
        {"data": "first", "links": [{"rel": "next", 'href': 'https://oeo.test/result-2', }]},
        {"data": "second", "links": [{"rel": "next", 'href': 'https://oeo.test/resulthr33', }]},
        {"data": "third"},
    ]


def test_paginate_no_links(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(API_URL + "results", json={"data": "d6t6"})
    con = Connection(API_URL)
    res = paginate(con, API_URL + "results")
    assert isinstance(res, typing.Iterator)
    assert list(res) == [{"data": "d6t6"}, ]


def test_paginate_params(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(
        API_URL + "result/1?bbox=square", complete_qs=True,
        json={"data": "first", "links": [{"rel": "next", "href": API_URL + "result-2?_orig_bbox=square"}]}
    )
    requests_mock.get(
        API_URL + "result-2?_orig_bbox=square", complete_qs=True,
        json={"data": "second", "links": [{"rel": "next", "href": API_URL + "resulthr33?_orig_bbox=square"}]}
    )
    requests_mock.get(
        API_URL + "resulthr33?_orig_bbox=square", complete_qs=True,
        json={"data": "third"}
    )
    con = Connection(API_URL)
    res = paginate(con, API_URL + "result/1", params={"bbox": "square"})
    assert isinstance(res, typing.Iterator)
    assert list(res) == [
        {"data": "first", "links": [{"rel": "next", 'href': 'https://oeo.test/result-2?_orig_bbox=square', }]},
        {"data": "second", "links": [{"rel": "next", 'href': 'https://oeo.test/resulthr33?_orig_bbox=square', }]},
        {"data": "third"},
    ]


def test_paginate_callback(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(
        API_URL + "result/1",
        json={"data": "first", "links": [{"rel": "next", "href": API_URL + "result-2"}]}
    )
    requests_mock.get(
        API_URL + "result-2",
        json={"data": "second", "links": [{"rel": "next", "href": API_URL + "resulthr33"}]}
    )
    requests_mock.get(
        API_URL + "resulthr33",
        json={"data": "third"}
    )
    con = Connection(API_URL)
    res = paginate(con, API_URL + "result/1", callback=lambda resp, page: (page, resp["data"]))
    assert isinstance(res, typing.Iterator)
    assert list(res) == [(1, "first"), (2, "second"), (3, "third")]


@pytest.mark.parametrize("data_factory", [
    lambda con: {"add1": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}},
    lambda con: con.datacube_from_process("add", x=3, y=5),
    lambda con: PGNode("add", x=3, y=5)
])
def test_as_curl(requests_mock, data_factory):
    requests_mock.get(API_URL, json={"api_version": "1.0.0", "endpoints": BASIC_ENDPOINTS})
    requests_mock.get(API_URL + 'credentials/basic', json={"access_token": "s3cr6t"})
    con = Connection(API_URL).authenticate_basic("john", "j0hn")

    data = data_factory(con)
    cmd = con.as_curl(data)
    assert cmd == (
        "curl -i -X POST -H 'Content-Type: application/json' -H 'Authorization: Bearer basic//s3cr6t' "
        '--data \'{"process":{"process_graph":{"add1":{"process_id":"add","arguments":{"x":3,"y":5},"result":true}}}}\' '
        "https://oeo.test/result"
    )


@pytest.fixture
def custom_client_config(tmp_path):
    """Fixture to create a fresh client config that will be loaded before test (and unloaded after test)."""
    config_path = tmp_path / "openeo-client-config.ini"
    config_path.write_text("")
    with mock.patch.dict(os.environ, {"OPENEO_CLIENT_CONFIG": str(config_path)}):
        # Force reloading of global (lazy loaded) config
        import openeo.config
        openeo.config._global_config = None
        yield config_path
        openeo.config._global_config = None


@pytest.mark.parametrize(["verbose", "on_stdout"], [
    ("print", True),
    ("off", False),
])
@pytest.mark.parametrize("use_default", [False, True])
def test_connect_default_backend_from_config(
        requests_mock, custom_client_config, caplog, capsys,
        verbose, on_stdout, use_default
):
    caplog.set_level(logging.INFO)
    default = f"https://openeo{random.randint(0, 1000)}.test"
    other = f"https://other{random.randint(0, 1000)}.test"
    custom_client_config.write_text(textwrap.dedent(f"""
        [General]
        verbose = {verbose}
        [Connection]
        default_backend = {default}
    """))
    requests_mock.get(default, json={"api_version": "1.0.0"})
    requests_mock.get(other, json={"api_version": "1.0.0"})

    expected_log = f"Using default back-end URL {default!r} (from config)"
    if use_default:
        # Without arguments: use default
        con = connect()
        assert con.root_url == default
        assert expected_log in caplog.text
        assert (expected_log in capsys.readouterr().out) == on_stdout
    else:
        # With url argument: still works, and no config related output
        con = connect(other)
        assert con.root_url == other
        assert expected_log not in caplog.text
        assert expected_log not in capsys.readouterr().out


@pytest.mark.parametrize(["verbose", "on_stdout"], [
    ("print", True),
    ("off", False),
])
@pytest.mark.parametrize(["auto_auth_config", "use_default", "authenticated"], [
    ("auto_authenticate", True, True),
    ("auto_authenticate", False, True),
    ("default_backend.auto_authenticate", True, True),
    ("default_backend.auto_authenticate", False, False),
])
def test_connect_auto_auth_from_config_basic(
        requests_mock, custom_client_config, auth_config, caplog, capsys,
        verbose, on_stdout, auto_auth_config, use_default, authenticated,
):
    caplog.set_level(logging.INFO)
    default = f"https://openeo{random.randint(0, 1000)}.test"
    other = f"https://other{random.randint(0, 1000)}.test"
    custom_client_config.write_text(textwrap.dedent(f"""
        [General]
        verbose = {verbose}
        [Connection]
        default_backend = {default}
        {auto_auth_config} = basic
    """))
    user, pwd = "john", "j0hn"
    for u, a in [(default, "Hell0!"), (other, "Wazz6!")]:
        auth_config.set_basic_auth(backend=u, username=user, password=pwd)
        requests_mock.get(u, json={"api_version": "1.0.0", "endpoints": BASIC_ENDPOINTS})
        requests_mock.get(f"{u}/credentials/basic", text=_credentials_basic_handler(user, pwd, access_token=a))

    if use_default:
        # Without arguments: use default
        con = connect()
        assert con.root_url == default
    else:
        # With url argument: still works (possibly with auto_auth too)
        con = connect(other)
        assert con.root_url == other

    expected_log = f"Doing auto-authentication 'basic' (from config)"
    if authenticated:
        assert isinstance(con.auth, BearerAuth)
        assert con.auth.bearer == "basic//Hell0!" if use_default else "basic//Wazz6!"
        assert expected_log in caplog.text
        assert (expected_log in capsys.readouterr().out) == on_stdout
    else:
        assert isinstance(con.auth, NullAuth)
        assert expected_log not in caplog.text
        assert expected_log not in capsys.readouterr().out


@pytest.mark.parametrize(["verbose", "on_stdout"], [
    ("print", True),
    ("off", False),
])
@pytest.mark.parametrize(["auto_auth_config", "use_default", "authenticated"], [
    ("auto_authenticate", True, True),
    ("auto_authenticate", False, True),
    ("default_backend.auto_authenticate", True, True),
    ("default_backend.auto_authenticate", False, False),
])
def test_connect_auto_auth_from_config_oidc_refresh_token(
        requests_mock, custom_client_config, auth_config, refresh_token_store, caplog, capsys,
        verbose, on_stdout, auto_auth_config, use_default, authenticated,
):
    """Auto-authorize with client config, auth config and refresh tokens"""
    caplog.set_level(logging.INFO)
    default = f"https://openeo{random.randint(0, 1000)}.test"
    other = f"https://other{random.randint(0, 1000)}.test"
    client_id = "myclient"
    refresh_token = "r3fr35h!"
    issuer = "https://oidc.test"

    custom_client_config.write_text(textwrap.dedent(f"""
        [General]
        verbose = {verbose}
        [Connection]
        default_backend = {default}
        {auto_auth_config} = oidc
    """))

    for u in [default, other]:
        requests_mock.get(u, json={"api_version": "1.0.0"})
        requests_mock.get(f"{u}/credentials/oidc", json={
            "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
        })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=issuer,
        expected_fields={"refresh_token": refresh_token}
    )
    refresh_token_store.set_refresh_token(issuer=issuer, client_id=client_id, refresh_token=refresh_token)
    auth_config.set_oidc_client_config(backend=default, provider_id="oi", client_id=client_id)
    auth_config.set_oidc_client_config(backend=other, provider_id="oi", client_id=client_id)

    # With all this set up, now the real work:
    if use_default:
        con = connect()
        assert con.root_url == default
    else:
        con = connect(other)
        assert con.root_url == other

    expected_log = f"Doing auto-authentication 'oidc' (from config)"
    if authenticated:
        assert isinstance(con.auth, BearerAuth)
        assert con.auth.bearer == "oidc/oi/" + oidc_mock.state["access_token"]
        assert expected_log in caplog.text
        assert (expected_log in capsys.readouterr().out) == on_stdout
    else:
        assert isinstance(con.auth, NullAuth)
        assert expected_log not in caplog.text
        assert expected_log not in capsys.readouterr().out


@pytest.mark.parametrize(["auto_auth_config", "use_default", "authenticated"], [
    ("auto_authenticate", True, True),
    ("auto_authenticate", False, True),
    ("default_backend.auto_authenticate", True, True),
    ("default_backend.auto_authenticate", False, False),
])
def test_connect_auto_auth_from_config_oidc_device_code(
    requests_mock,
    custom_client_config,
    auth_config,
    caplog,
    auto_auth_config,
    use_default,
    authenticated,
    oidc_device_code_flow_checker,
    fast_sleep,
):
    """Auto-authorize without auth config or refresh tokens"""
    caplog.set_level(logging.INFO)
    default = f"https://openeo{random.randint(0, 1000)}.test"
    other = f"https://other{random.randint(0, 1000)}.test"
    default_client_id = "dadefaultklient"
    grant_types = ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
    oidc_issuer = "https://auth.test"

    custom_client_config.write_text(textwrap.dedent(f"""
        [Connection]
        default_backend = {default}
        {auto_auth_config} = oidc
    """))

    for u in [default, other]:
        requests_mock.get(u, json={"api_version": "1.0.0"})
        requests_mock.get(
            f"{u}/credentials/oidc",
            json={
                "providers": [
                    {
                        "id": "auth",
                        "issuer": oidc_issuer,
                        "title": "Auth",
                        "scopes": ["openid"],
                        "default_clients": [{"id": default_client_id, "grant_types": grant_types}],
                    },
                ]
            },
        )

    expected_fields = {
        "scope": "openid",
        "code_verifier": True,
        "code_challenge": True
    }
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=default_client_id,
        expected_fields=expected_fields,
        scopes_supported=["openid"],
        oidc_issuer=oidc_issuer,
    )
    assert auth_config.load() == {}

    # With all this set up, now the real work:
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    if authenticated:
        ctx = oidc_device_code_flow_checker(url=f"{oidc_issuer}/dc")
    else:
        ctx = nullcontext()
    with ctx:
        if use_default:
            con = connect()
            assert con.root_url == default
        else:
            con = connect(other)
            assert con.root_url == other

    expected_log = f"Doing auto-authentication 'oidc' (from config)"
    if authenticated:
        assert isinstance(con.auth, BearerAuth)
        assert con.auth.bearer == "oidc/auth/" + oidc_mock.state["access_token"]
        assert expected_log in caplog.text
    else:
        assert isinstance(con.auth, NullAuth)
        assert expected_log not in caplog.text


@pytest.mark.parametrize(["capabilities", "expected"], [
    (
            {"api_version": "1.0.0"},
            {"api": "1.0.0", "backend": {"root_url": "https://oeo.test/"}, "client": openeo.client_version()}
    ),
    (
            {"api_version": "1.1.0"},
            {"api": "1.1.0", "backend": {"root_url": "https://oeo.test/"}, "client": openeo.client_version()}
    ),
    (
            {"api_version": "1.1.0", "backend_version": "1.2.3", "processing:software": {"foolib": "4.5.6"}},
            {
                "api": "1.1.0",
                "backend": {
                    "root_url": "https://oeo.test/", "version": "1.2.3", "processing:software": {"foolib": "4.5.6"}
                },
                "client": openeo.client_version(),
            }
    ),
])
def test_version_info(requests_mock, capabilities, expected):
    requests_mock.get("https://oeo.test/", json=capabilities)
    con = Connection(API_URL)
    assert con.version_info() == expected


def test_vectorcube_from_paths(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    con = Connection(API_URL)
    vc = con.vectorcube_from_paths(["mydata.pq"], format="parquet")
    assert vc.flat_graph() == {
        "loaduploadedfiles1": {
            "process_id": "load_uploaded_files",
            "arguments": {"paths": ["mydata.pq"], "format": "parquet", "options": {}},
            "result": True,
        }
    }


class TestExecuteFromJsonResources:
    """
    Tests for executing process graphs directly from JSON resources (JSON dumps, files, URLs, ...)
    """
    # Dummy process graphs
    PG_JSON_1 = '{"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}}'
    PG_DICT_1 = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
    PG_JSON_2 = '{"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}}}'

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_download_pg_json(self, dummy_backend, connection, tmp_path, pg_json: str):
        output = tmp_path / "result.tiff"
        connection.download(pg_json, outputfile=output)
        assert output.read_bytes() == b'{"what?": "Result data"}'
        assert dummy_backend.get_sync_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_execute_pg_json(self, dummy_backend, connection, pg_json: str):
        dummy_backend.next_result = {"answer": 8}
        result = connection.execute(pg_json)
        assert result == {"answer": 8}
        assert dummy_backend.get_sync_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_create_job_pg_json(self, dummy_backend, connection, pg_json: str):
        job = connection.create_job(pg_json)
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    @pytest.mark.parametrize("path_factory", [str, Path])
    def test_download_pg_json_file(self, dummy_backend, connection, tmp_path, pg_json: str, path_factory):
        json_file = tmp_path / "input.json"
        json_file.write_text(pg_json)
        json_file = path_factory(json_file)

        output = tmp_path / "result.tiff"
        connection.download(json_file, outputfile=output)
        assert output.read_bytes() == b'{"what?": "Result data"}'
        assert dummy_backend.get_sync_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    @pytest.mark.parametrize("path_factory", [str, Path])
    def test_execute_pg_json_file(self, dummy_backend, connection, pg_json: str, tmp_path, path_factory):
        dummy_backend.next_result = {"answer": 8}

        json_file = tmp_path / "input.json"
        json_file.write_text(pg_json)
        json_file = path_factory(json_file)

        result = connection.execute(json_file)
        assert result == {"answer": 8}
        assert dummy_backend.get_sync_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    @pytest.mark.parametrize("path_factory", [str, Path])
    def test_create_job_pg_json_file(self, dummy_backend, connection, pg_json: str, tmp_path, path_factory):
        json_file = tmp_path / "input.json"
        json_file.write_text(pg_json)
        json_file = path_factory(json_file)

        job = connection.create_job(json_file)
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_download_pg_json_url(self, dummy_backend, connection, requests_mock, tmp_path, pg_json: str):
        url = "https://jsonbin.test/pg.json"
        requests_mock.get(url, text=pg_json)

        output = tmp_path / "result.tiff"
        connection.download(url, outputfile=output)
        assert output.read_bytes() == b'{"what?": "Result data"}'
        assert dummy_backend.get_sync_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_execute_pg_json_url(self, dummy_backend, connection, requests_mock, pg_json: str):
        dummy_backend.next_result = {"answer": 8}

        url = "https://jsonbin.test/pg.json"
        requests_mock.get(url, text=pg_json)

        result = connection.execute(url)
        assert result == {"answer": 8}
        assert dummy_backend.get_sync_pg() == self.PG_DICT_1

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_create_job_pg_json_url(self, dummy_backend, connection, requests_mock, pg_json: str):
        url = "https://jsonbin.test/pg.json"
        requests_mock.get(url, text=pg_json)

        job = connection.create_job(url)
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self.PG_DICT_1


class TestExecuteWithValidation:
    # Dummy process graphs
    PG_DICT_1 = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}

    @pytest.fixture(params=[False, True])
    def auto_validate(self, request) -> bool:
        """Fixture to parametrize auto_validate setting."""
        return request.param

    @pytest.fixture
    def connection(self, api_version, requests_mock, api_capabilities, auto_validate) -> Connection:
        requests_mock.get(API_URL, json=build_capabilities(api_version=api_version, **api_capabilities))
        con = Connection(API_URL, **dict_no_none(auto_validate=auto_validate))
        return con

    # Reusable list of (fixture) parameterization
    # of ["api_capabilities", "auto_validate", "validate", "validation_expected"]
    _VALIDATION_PARAMETER_SETS = [
        # No validation supported by backend: don't attempt to validate
        ({}, None, None, False),
        ({}, True, True, False),
        # Validation supported by backend, default behavior -> validate
        ({"validation": True}, None, None, True),
        # (Validation supported by backend) no explicit validation enabled: follow auto_validate setting
        ({"validation": True}, True, None, True),
        ({"validation": True}, False, None, False),
        # (Validation supported by backend) follow explicit `validate` toggle regardless of auto_validate
        ({"validation": True}, False, True, True),
        ({"validation": True}, True, False, False),
    ]

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_download_validation(
        self,
        dummy_backend,
        connection,
        tmp_path,
        caplog,
        api_capabilities,
        validate,
        validation_expected,
    ):
        caplog.set_level(logging.WARNING)
        dummy_backend.next_result = b"TIFF data"
        dummy_backend.next_validation_errors = [
            {"code": "OddSupport", "message": "Odd values are not supported."},
            {"code": "ComplexityOverflow", "message": "Too complex."},
        ]

        output = tmp_path / "result.tiff"
        connection.download(self.PG_DICT_1, outputfile=output, **dict_no_none(validate=validate))
        assert output.read_bytes() == b"TIFF data"
        assert dummy_backend.get_sync_pg() == self.PG_DICT_1

        if validation_expected:
            assert caplog.messages == [
                "Preflight process graph validation raised: [OddSupport] Odd values are not supported. [ComplexityOverflow] Too complex."
            ]
            assert dummy_backend.validation_requests == [self.PG_DICT_1]
        else:
            assert caplog.messages == []
            assert dummy_backend.validation_requests == []

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate"],
        [
            ({"validation": True}, True),
        ],
    )
    def test_download_validation_broken(
        self, dummy_backend, connection, requests_mock, tmp_path, caplog, api_capabilities
    ):
        """
        Verify the job won't be blocked if errors occur during validation that are
        not related to the validity of the graph, e.g. the HTTP request itself fails, etc.
        Because we don't want to break the existing workflows.
        """
        caplog.set_level(logging.WARNING)
        dummy_backend.next_result = b"TIFF data"

        # Simulate server side error during the request.
        m = requests_mock.post(API_URL + "validation", json={"code": "Internal", "message": "Nope!"}, status_code=500)

        output = tmp_path / "result.tiff"
        connection.download(self.PG_DICT_1, outputfile=output, validate=True)
        assert output.read_bytes() == b"TIFF data"

        # We still want to see those warnings in the logs though:
        assert caplog.messages == ["Preflight process graph validation failed: [500] Internal: Nope!"]
        assert m.call_count == 1

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_execute_validation(
        self, dummy_backend, connection, caplog, api_capabilities, validate, validation_expected
    ):
        caplog.set_level(logging.WARNING)
        dummy_backend.next_result = {"answer": 8}
        dummy_backend.next_validation_errors = [{"code": "OddSupport", "message": "Odd values are not supported."}]

        result = connection.execute(self.PG_DICT_1, **dict_no_none(validate=validate))
        assert result == {"answer": 8}
        if validation_expected:
            assert caplog.messages == [
                "Preflight process graph validation raised: [OddSupport] Odd values are not supported."
            ]
            assert dummy_backend.validation_requests == [self.PG_DICT_1]
        else:
            assert caplog.messages == []
            assert dummy_backend.validation_requests == []

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_create_job_validation(
        self, dummy_backend, connection, caplog, api_capabilities, validate, validation_expected
    ):
        caplog.set_level(logging.WARNING)
        dummy_backend.next_validation_errors = [{"code": "OddSupport", "message": "Odd values are not supported."}]

        job = connection.create_job(self.PG_DICT_1, **dict_no_none(validate=validate))
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self.PG_DICT_1

        if validation_expected:
            assert caplog.messages == [
                "Preflight process graph validation raised: [OddSupport] Odd values are not supported."
            ]
            assert dummy_backend.validation_requests == [self.PG_DICT_1]
        else:
            assert caplog.messages == []
            assert dummy_backend.validation_requests == []

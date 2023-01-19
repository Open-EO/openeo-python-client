import json
import logging
import os
import random
import re
import textwrap
import typing
import unittest.mock as mock
import zlib
from pathlib import Path

import pytest
import requests.auth
import requests_mock

import openeo
from openeo.capabilities import ComparableVersion
from openeo.internal.graph_building import PGNode
from openeo.rest import OpenEoApiError, OpenEoClientException, OpenEoRestError
from openeo.rest.auth.auth import BearerAuth, NullAuth
from openeo.rest.auth.oidc import OidcException
from openeo.rest.auth.testing import ABSENT, OidcMock, assert_device_code_poll_sleep
from openeo.rest.connection import Connection, RestApiConnection, connect, paginate
from openeo.util import ContextTimer

from .. import load_json_resource
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
    with pytest.raises(OpenEoApiError, match="Consider.*batch jobs.*instead.*synchronous"):
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
        url="https://oeo.test/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY, timeout=None
    )


def test_connect_with_session():
    session = mock.Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {"foo": "bar", "api_version": "1.0.0"}
    conn = connect("https://oeo.test/", session=session)
    assert conn.capabilities().capabilities["foo"] == "bar"
    session.request.assert_any_call(
        url="https://oeo.test/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY, timeout=None
    )


@pytest.mark.parametrize(["versions", "expected_url", "expected_version"], [
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.test/openeo/0.4.1/"},
                {"api_version": "0.4.2", "url": "https://oeo.test/openeo/0.4.2/"},
                {"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.0.0/"},
            ],
            "https://oeo.test/openeo/1.0.0/",
            "1.0.0",
    ),
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.test/openeo/0.4.1/"},
                {"api_version": "0.4.2", "url": "https://oeo.test/openeo/0.4.2/"},
                {"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.0.0/", "production": False},
            ],
            "https://oeo.test/openeo/0.4.2/",
            "0.4.2",
    ),
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.test/openeo/0.4.1/", "production": True},
                {"api_version": "0.4.2", "url": "https://oeo.test/openeo/0.4.2/", "production": True},
                {"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.0.0/", "production": False},
            ],
            "https://oeo.test/openeo/0.4.2/",
            "0.4.2",
    ),
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.test/openeo/0.4.1/", "production": False},
                {"api_version": "0.4.2", "url": "https://oeo.test/openeo/0.4.2/", "production": False},
                {"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.0.0/", "production": False},
            ],
            "https://oeo.test/openeo/1.0.0/",
            "1.0.0",
    ),
    (
            [
                {"api_version": "0.1.0", "url": "https://oeo.test/openeo/0.1.0/", "production": True},
                {"api_version": "0.4.2", "url": "https://oeo.test/openeo/0.4.2/", "production": False},
            ],
            "https://oeo.test/openeo/0.4.2/",
            "0.4.2",
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
    requests_mock.get('https://oeo.test/', json={"api_version": "0.4.0"})
    requests_mock.get('https://oeo.test/collections/foobar', status_code=404, json={
        "code": "CollectionNotFound", "message": "No such thing as a collection 'foobar'", "id": "54321",
    })
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
    requests_mock.get('https://oeo.test/', json={"api_version": "0.4.0"})
    requests_mock.get('https://oeo.test/collections/foobar', status_code=404, json={
        "code": "CollectionNotFound", "message": "No such thing as a collection 'foobar'",
    })
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
    requests_mock.get('https://oeo.test/', json={"api_version": "0.4.0"})
    conn = Connection(API_URL)
    requests_mock.get('https://oeo.test/collections/foobar', status_code=500, text="olapola")
    with pytest.raises(OpenEoApiError) as exc_info:
        conn.describe_collection("foobar")
    exc = exc_info.value
    assert exc.http_status_code == 500
    assert exc.code == "unknown"
    assert exc.message == "olapola"
    assert exc.id is None
    assert exc.url is None


def _credentials_basic_handler(username, password, access_token="w3lc0m3"):
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
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret},
        oidc_issuer=issuer,
    )

    with mock.patch('openeo.rest.connection.RefreshTokenStore') as RefreshTokenStore:
        conn = Connection(API_URL)
        assert RefreshTokenStore.call_count == 0
        # Create RefreshTokenStore lazily when necessary
        conn.authenticate_oidc_client_credentials(
            client_id=client_id, client_secret=client_secret, store_refresh_token=True
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
    if ComparableVersion(api_version).at_least("1.0.0"):
        assert conn.auth.bearer == "basic//w3lc0m3"
    else:
        assert conn.auth.bearer == "w3lc0m3"


def test_authenticate_basic_from_config(requests_mock, api_version, auth_config):
    user, pwd = "john281", "J0hndo3"
    requests_mock.get(API_URL, json={"api_version": api_version, "endpoints": BASIC_ENDPOINTS})
    requests_mock.get(API_URL + 'credentials/basic', text=_credentials_basic_handler(user, pwd))
    auth_config.set_basic_auth(backend=API_URL, username=user, password=pwd)

    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_basic()
    assert isinstance(conn.auth, BearerAuth)
    if ComparableVersion(api_version).at_least("1.0.0"):
        assert conn.auth.bearer == "basic//w3lc0m3"
    else:
        assert conn.auth.bearer == "w3lc0m3"


@pytest.mark.slow
def test_authenticate_oidc_authorization_code_040(requests_mock):
    client_id = "myclient"
    oidc_discovery_url = "https://oeo.test/credentials/oidc"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid"},
        oidc_discovery_url=oidc_discovery_url
    )
    requests_mock.get(API_URL, json={"api_version": "0.4.0"})

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_authorization_code(client_id=client_id, webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == oidc_mock.state["access_token"]


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
        expected_fields={"client_secret": client_secret},
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
    conn.authenticate_oidc_client_credentials(
        client_id=client_id, client_secret=client_secret, store_refresh_token=True
    )
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == [
        mock.call.set_refresh_token(client_id=client_id, issuer=issuer, refresh_token=oidc_mock.state["refresh_token"])
    ]


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
        expected_fields={"client_secret": client_secret},
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
        requests_mock, store_refresh_token, scopes_supported, expected_scopes
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
    with assert_device_code_poll_sleep():
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


def test_authenticate_oidc_device_flow_with_secret_from_config(requests_mock, auth_config, caplog):
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
    with assert_device_code_poll_sleep():
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
        requests_mock, auth_config, caplog, use_pkce, expect_pkce
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
        oidc_issuer="https://fauth.test",
    )
    assert auth_config.load() == {}

    # With all this set up, kick off the openid connect flow
    caplog.set_level(logging.INFO)
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with assert_device_code_poll_sleep():
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
        requests_mock, auth_config, caplog, use_pkce, expect_pkce
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
        oidc_issuer="https://fauth.test",
    )
    assert auth_config.load() == {}
    auth_config.set_oidc_client_config(backend=API_URL, provider_id="fauth", client_id=client_id)

    # With all this set up, kick off the openid connect flow
    caplog.set_level(logging.INFO)
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with assert_device_code_poll_sleep():
        conn.authenticate_oidc_device(use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/fauth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []
    assert "No OIDC provider given, but only one in config (for backend 'https://oeo.test/'): 'fauth'. Using that one." in caplog.text
    assert "Using client_id 'myclient' from config (provider 'fauth')" in caplog.text


def test_authenticate_oidc_device_flow_pkce_multiple_provider_one_config_no_given_default_client(requests_mock, auth_config):
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
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=default_client_id,
        expected_fields={
            "scope": "openid", "code_verifier": True, "code_challenge": True
        },
        scopes_supported=["openid"],
        oidc_issuer="https://bauth.test",
    )
    assert auth_config.load() == {}
    auth_config.set_oidc_client_config(backend=API_URL, provider_id="bauth", client_id=None)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with assert_device_code_poll_sleep():
        conn.authenticate_oidc_device()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/bauth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.parametrize(["grant_types", "use_pkce", "expect_pkce"], [
    (["urn:ietf:params:oauth:grant-type:device_code+pkce"], True, True),
    (["urn:ietf:params:oauth:grant-type:device_code+pkce"], None, True),
    (["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"], None, True),
    (["urn:ietf:params:oauth:grant-type:device_code"], None, False),
    (["urn:ietf:params:oauth:grant-type:device_code"], False, False),
])
def test_authenticate_oidc_device_flow_pkce_default_client_handling(requests_mock, grant_types, use_pkce, expect_pkce):
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
    with assert_device_code_poll_sleep():
        conn.authenticate_oidc_device(use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/auth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


def test_authenticate_oidc_device_flow_pkce_store_refresh_token(requests_mock):
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
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=default_client_id,
        expected_fields=expected_fields,
        scopes_supported=["openid"],
        oidc_issuer="https://auth.test",
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with assert_device_code_poll_sleep():
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


@pytest.mark.parametrize(["use_pkce", "expect_pkce"], [
    (None, False),
    (True, True),
    (False, False),
])
def test_authenticate_oidc_auto_no_existing_refresh_token(requests_mock, refresh_token_store, use_pkce, expect_pkce):
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
    with assert_device_code_poll_sleep():
        conn.authenticate_oidc(client_id=client_id, use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == [
        "urn:ietf:params:oauth:grant-type:device_code"
    ]


@pytest.mark.parametrize(["use_pkce", "expect_pkce"], [
    (None, False),
    (True, True),
    (False, False),
])
def test_authenticate_oidc_auto_expired_refresh_token(requests_mock, refresh_token_store, use_pkce, expect_pkce):
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
    with assert_device_code_poll_sleep():
        conn.authenticate_oidc(client_id=client_id, use_pkce=use_pkce)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == [
        "refresh_token",
        "urn:ietf:params:oauth:grant-type:device_code",
    ]
    assert oidc_mock.grant_request_history[0]["response"] == {"error": "invalid refresh token"}


def _setup_get_me_handler(requests_mock, oidc_mock: OidcMock):
    def get_me(request: requests.Request, context):
        """handler for `GET /me` (with access_token checking)"""
        auth_header = request.headers["Authorization"]
        oidc_provider, access_token = re.match(r"Bearer oidc/(?P<p>\w+)/(?P<a>.*)", auth_header).group("p", "a")
        try:
            user_id = oidc_mock.validate_access_token(access_token)["user_id"]
        except LookupError:
            context.status_code = 403
            return {"code": "TokenInvalid"}

        return {
            "user_id": user_id,
            # Normally not part of `GET /me` response, but just for test/inspection purposes
            "_used_oidc_provider": oidc_provider,
            "_used_access_token": access_token,
        }

    requests_mock.get(API_URL + "me", json=get_me)


@pytest.mark.parametrize(["invalidate"], [
    (False,),
    (True,),
])
def test_authenticate_oidc_auto_refresh_expired_access_token_initial_refresh_token(
        requests_mock, refresh_token_store, invalidate, caplog
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
    _setup_get_me_handler(requests_mock=requests_mock, oidc_mock=oidc_mock)
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
    assert "[403] TokenInvalid" not in caplog.text
    get_me_response = conn.describe_account()
    access_token2 = oidc_mock.state["access_token"]
    assert "refresh_token" not in oidc_mock.state
    if invalidate:
        # Two "refresh_token" auth requests should have happened now
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["refresh_token", "refresh_token"]
        assert access_token2 != access_token1
        assert "expired access token ([403] TokenInvalid)" in caplog.text
        assert "automatically re-authenticated with refresh token" in caplog.text
    else:
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == ["refresh_token"]
        assert access_token2 == access_token1
        assert "[403] TokenInvalid" not in caplog.text

    assert get_me_response == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token2,
    }


@pytest.mark.parametrize(["invalidate"], [
    (False,),
    (True,),
])
def test_authenticate_oidc_auto_refresh_expired_access_token_initial_device_code(
        requests_mock, refresh_token_store, invalidate, caplog
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
    _setup_get_me_handler(requests_mock=requests_mock, oidc_mock=oidc_mock)
    caplog.set_level(logging.INFO)

    # Explicit authentication with `authenticate_oidc_device`
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with assert_device_code_poll_sleep():
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
    assert "[403] TokenInvalid" not in caplog.text
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
        assert "expired access token ([403] TokenInvalid)" in caplog.text
        assert "automatically re-authenticated with refresh token" in caplog.text
    else:
        assert [h["grant_type"] for h in oidc_mock.grant_request_history] == [
            "urn:ietf:params:oauth:grant-type:device_code"
        ]
        assert (access_token2, refresh_token2) == (access_token1, refresh_token1)
        assert "[403] TokenInvalid" not in caplog.text

    assert get_me_response == {
        "user_id": "john",
        "_used_oidc_provider": "oi",
        "_used_access_token": access_token2,
    }


def test_authenticate_oidc_auto_refresh_expired_access_token_invalid_refresh_token(
        requests_mock, refresh_token_store, caplog
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
    _setup_get_me_handler(requests_mock=requests_mock, oidc_mock=oidc_mock)
    caplog.set_level(logging.INFO)

    # Explicit authentication with `authenticate_oidc_device`
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with assert_device_code_poll_sleep():
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
    assert "[403] TokenInvalid" not in caplog.text
    with pytest.raises(OpenEoApiError):
        conn.describe_account()

    assert "expired access token ([403] TokenInvalid)" in caplog.text
    assert "failed to automatically re-authenticate using refresh token" in caplog.text


def test_authenticate_oidc_auto_refresh_expired_access_token_other_errors(
        requests_mock, caplog
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

    def get_me(request: requests.Request, context):
        """handler for `GET /me` (with access_token checking)"""
        context.status_code = 500
        return {"code": "Internal"}

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
    with pytest.raises(OpenEoApiError):
        conn.describe_account()
    # The re-authentication retry logic should not be triggered
    assert "re-auth" not in caplog.text


def test_load_collection_arguments_040(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "0.4.0"})
    conn = Connection(API_URL)
    requests_mock.get(API_URL + "collections/FOO", json={
        "properties": {"eo:bands": [{"name": "red"}, {"name": "green"}, {"name": "blue"}]}
    })
    spatial_extent = {"west": 1, "south": 2, "east": 3, "north": 4}
    temporal_extent = ["2019-01-01", "2019-01-22"]
    im = conn.load_collection(
        "FOO", spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=["red", "green"]
    )
    node = im.flat_graph()[im.node_id]
    assert node["process_id"] == "load_collection"
    assert node["arguments"] == {
        "id": "FOO",
        "spatial_extent": spatial_extent,
        "temporal_extent": temporal_extent,
        "bands": ["red", "green"]
    }


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
    m = requests_mock.get(API_URL + "file_formats", status_code=204, json={"message": "No content."})
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
    m = requests_mock.get(API_URL + "service_types", status_code=204, json={"message": "No content."})
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
    runtimes = {"Python": {"type": "language", "versions": {"3.7": {}}, "default": "3.7"}}
    m = requests_mock.get(API_URL + "udf_runtimes", json=runtimes)
    assert conn.list_udf_runtimes() == runtimes
    assert m.call_count == 1
    # Check caching
    assert conn.list_udf_runtimes() == runtimes
    assert m.call_count == 1


def test_list_udf_runtimes_error(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    m = requests_mock.get(API_URL + "udf_runtimes", status_code=204, json={"message": "No content."})
    with pytest.raises(OpenEoRestError):
        conn.list_udf_runtimes()
    assert m.call_count == 1
    # Error is not cached
    with pytest.raises(OpenEoRestError):
        conn.list_udf_runtimes()
    assert m.call_count == 2


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
    m = requests_mock.get(API_URL + "processes", status_code=204, json={"message": "No content."})
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
    assert conn.get("/foo").text == 'None'
    assert conn.get("/foo", timeout=5).text == '5'


def test_default_timeout(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get("/foo", json=lambda req, ctx: repr(req.timeout))
    conn = connect(API_URL, default_timeout=2)
    assert conn.get("/foo").json() == '2'
    assert conn.get("/foo", timeout=5).json() == '5'


def test_execute_042(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "0.4.2"})
    conn = Connection(API_URL)
    with mock.patch.object(conn, "request") as request:
        conn.execute({"foo1": {"process_id": "foo"}})
    assert request.call_args_list == [
        mock.call(
            "post", path="/result", allow_redirects=False, expected_status=200,
            json={"process_graph": {"foo1": {"process_id": "foo"}}}
        )
    ]


@pytest.mark.parametrize("pg", [
    {"foo1": {"process_id": "foo"}},
    {"process_graph": {"foo1": {"process_id": "foo"}}},
    type("", (object,), {"flat_graph": (lambda: {"foo1": {"process_id": "foo"}})})
])
def test_execute_100(requests_mock, pg):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    with mock.patch.object(conn, "request") as request:
        conn.execute(pg)
    assert request.call_args_list == [
        mock.call(
            "post", path="/result", allow_redirects=False, expected_status=200,
            json={"process": {"process_graph": {"foo1": {"process_id": "foo"}}}}
        )
    ]


def test_create_udp(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(API_URL + "processes", json={"processes": [{"id": "add"}]})
    conn = Connection(API_URL)

    new_udp = load_json_resource("data/1.0.0/udp_details.json")

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == new_udp['process_graph']
        assert body['parameters'] == new_udp['parameters']
        assert body['public'] is False
        return True

    adapter = requests_mock.put(API_URL + "process_graphs/evi", additional_matcher=check_body)

    conn.save_user_defined_process(
        user_defined_process_id='evi',
        process_graph=new_udp['process_graph'],
        parameters=new_udp['parameters']
    )

    assert adapter.called


def test_create_public_udp(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    requests_mock.get(API_URL + "processes", json={"processes": [{"id": "add"}]})
    conn = Connection(API_URL)

    new_udp = load_json_resource("data/1.0.0/udp_details.json")

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == new_udp['process_graph']
        assert body['parameters'] == new_udp['parameters']
        assert body['public'] is True
        return True

    adapter = requests_mock.put(API_URL + "process_graphs/evi", additional_matcher=check_body)

    conn.save_user_defined_process(
        user_defined_process_id='evi',
        process_graph=new_udp['process_graph'],
        parameters=new_udp['parameters'],
        public=True
    )

    assert adapter.called


def test_list_udps(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)

    udp = load_json_resource("data/1.0.0/udp_details.json")

    requests_mock.get(API_URL + "process_graphs", json={
        'processes': [udp]
    })

    user_udps = conn.list_user_defined_processes()

    assert len(user_udps) == 1
    assert user_udps[0] == udp


def test_get_udp(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)

    udp = conn.user_defined_process('evi')

    assert udp.user_defined_process_id == 'evi'


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
        requests_mock, custom_client_config, auth_config, caplog,
        auto_auth_config, use_default, authenticated,
):
    """Auto-authorize without auth config or refresh tokens"""
    caplog.set_level(logging.INFO)
    default = f"https://openeo{random.randint(0, 1000)}.test"
    other = f"https://other{random.randint(0, 1000)}.test"
    default_client_id = "dadefaultklient"
    grant_types = ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
    issuer = "https://auth.test"

    custom_client_config.write_text(textwrap.dedent(f"""
        [Connection]
        default_backend = {default}
        {auto_auth_config} = oidc
    """))

    for u in [default, other]:
        requests_mock.get(u, json={"api_version": "1.0.0"})
        requests_mock.get(f"{u}/credentials/oidc", json={
            "providers": [
                {
                    "id": "auth", "issuer": issuer, "title": "Auth", "scopes": ["openid"],
                    "default_clients": [{"id": default_client_id, "grant_types": grant_types}]
                },
            ]
        })

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
        oidc_issuer=issuer,
    )
    assert auth_config.load() == {}

    # With all this set up, now the real work:
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    with assert_device_code_poll_sleep(expect_called=authenticated):
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


class TestExecute:
    # Dummy process graphs
    PG_JSON_1 = '{"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}}'
    PG_JSON_2 = '{"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}}}'

    # Dummy `POST /result` handlers
    def _post_result_handler_tiff(self, response: requests.Request, context):
        pg = response.json()["process"]["process_graph"]
        assert pg == {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        return b"TIFF data"

    def _post_result_handler_json(self, response: requests.Request, context):
        pg = response.json()["process"]["process_graph"]
        assert pg == {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        return {"answer": 8}

    def _post_jobs_handler_json(self, response: requests.Request, context):
        pg = response.json()["process"]["process_graph"]
        assert pg == {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        context.headers["OpenEO-Identifier"] = "j-123"
        return b""

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_download_pg_json(self, requests_mock, tmp_path, pg_json: str):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "result", content=self._post_result_handler_tiff)

        conn = Connection(API_URL)
        output = tmp_path / "result.tiff"
        conn.download(pg_json, outputfile=output)
        assert output.read_bytes() == b"TIFF data"

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_execute_pg_json(self, requests_mock, pg_json: str):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "result", json=self._post_result_handler_json)

        conn = Connection(API_URL)
        result = conn.execute(pg_json)
        assert result == {"answer": 8}

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_create_job_pg_json(self, requests_mock, pg_json: str):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "jobs", status_code=201, content=self._post_jobs_handler_json)

        conn = Connection(API_URL)
        job = conn.create_job(pg_json)
        assert job.job_id == "j-123"

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    @pytest.mark.parametrize("path_factory", [str, Path])
    def test_download_pg_json_file(self, requests_mock, tmp_path, pg_json: str, path_factory):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "result", content=self._post_result_handler_tiff)
        json_file = tmp_path / "input.json"
        json_file.write_text(pg_json)
        json_file = path_factory(json_file)

        conn = Connection(API_URL)
        output = tmp_path / "result.tiff"
        conn.download(json_file, outputfile=output)
        assert output.read_bytes() == b"TIFF data"

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    @pytest.mark.parametrize("path_factory", [str, Path])
    def test_execute_pg_json_file(self, requests_mock, pg_json: str, tmp_path, path_factory):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "result", json=self._post_result_handler_json)
        json_file = tmp_path / "input.json"
        json_file.write_text(pg_json)
        json_file = path_factory(json_file)

        conn = Connection(API_URL)
        result = conn.execute(json_file)
        assert result == {"answer": 8}

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    @pytest.mark.parametrize("path_factory", [str, Path])
    def test_create_job_pg_json_file(self, requests_mock, pg_json: str, tmp_path, path_factory):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "jobs", status_code=201, content=self._post_jobs_handler_json)
        json_file = tmp_path / "input.json"
        json_file.write_text(pg_json)
        json_file = path_factory(json_file)

        conn = Connection(API_URL)
        job = conn.create_job(json_file)
        assert job.job_id == "j-123"

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_download_pg_json_url(self, requests_mock, tmp_path, pg_json: str):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "result", content=self._post_result_handler_tiff)
        url = "https://jsonbin.test/pg.json"
        requests_mock.get(url, text=pg_json)

        conn = Connection(API_URL)
        output = tmp_path / "result.tiff"
        conn.download(url, outputfile=output)
        assert output.read_bytes() == b"TIFF data"

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_execute_pg_json_url(self, requests_mock, pg_json: str):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "result", json=self._post_result_handler_json)
        url = "https://jsonbin.test/pg.json"
        requests_mock.get(url, text=pg_json)

        conn = Connection(API_URL)
        result = conn.execute(url)
        assert result == {"answer": 8}

    @pytest.mark.parametrize("pg_json", [PG_JSON_1, PG_JSON_2])
    def test_create_job_pg_json_url(self, requests_mock, pg_json: str):
        requests_mock.get(API_URL, json={"api_version": "1.0.0"})
        requests_mock.post(API_URL + "jobs", status_code=201, content=self._post_jobs_handler_json)
        url = "https://jsonbin.test/pg.json"
        requests_mock.get(url, text=pg_json)

        conn = Connection(API_URL)
        job = conn.create_job(url)
        assert job.job_id == "j-123"

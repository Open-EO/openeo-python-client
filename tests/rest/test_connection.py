import re
import typing
import unittest.mock as mock
import zlib
from pathlib import Path

import pytest
import requests.auth
import requests_mock

from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoClientException, OpenEoApiError
from openeo.rest.auth.auth import NullAuth, BearerAuth
from openeo.rest.auth.oidc import OidcException
from openeo.rest.connection import Connection, RestApiConnection, connect, paginate
from .auth.test_cli import auth_config, refresh_token_store
from .auth.test_oidc import OidcMock
from .. import load_json_resource

API_URL = "https://oeo.test/"

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


def test_connection_other_domain_auth_headers(requests_mock, api_version):
    """https://github.com/Open-EO/openeo-python-client/issues/201"""
    secret = "!secret token!"

    def debug(request: requests.Request, context):
        return repr(("hello world", request.headers))

    requests_mock.get(API_URL, json={"api_version": api_version})
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


def test_connection_repr(requests_mock):
    requests_mock.get("https://oeo.test/", status_code=404)
    requests_mock.get("https://oeo.test/.well-known/openeo", status_code=200, json={
        "versions": [{"api_version": "1.0.0", "url": "https://oeo.test/openeo/1.x/", "production": True}],
    })
    requests_mock.get("https://oeo.test/openeo/1.x/", status_code=200, json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/openeo/1.x/credentials/basic", json={"access_token": "w3lc0m3"})

    conn = connect("https://oeo.test/")
    assert repr(conn) == "<Connection to 'https://oeo.test/openeo/1.x/' with NullAuth>"
    conn.authenticate_basic("foo", "bar")
    assert repr(conn) == "<Connection to 'https://oeo.test/openeo/1.x/' with BearerAuth>"


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
    conn = Connection(API_URL)
    requests_mock.get('https://oeo.test/collections/foobar', status_code=404, json={
        "code": "CollectionNotFound", "message": "No such things as a collection 'foobar'", "id": "54321"
    })
    with pytest.raises(OpenEoApiError) as exc_info:
        conn.describe_collection("foobar")
    exc = exc_info.value
    assert exc.http_status_code == 404
    assert exc.code == "CollectionNotFound"
    assert exc.message == "No such things as a collection 'foobar'"
    assert exc.id == "54321"
    assert exc.url is None


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


def test_create_connection_lazy_auth_config(requests_mock, api_version):
    user, pwd = "john262", "J0hndo3"
    requests_mock.get(API_URL, json={"api_version": api_version})

    def text_callback(request, context):
        assert request.headers["Authorization"] == requests.auth._basic_auth_str(username=user, password=pwd)
        return '{"access_token":"w3lc0m3"}'

    requests_mock.get(API_URL + 'credentials/basic', text=text_callback)

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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret},
        oidc_discovery_url=oidc_discovery_url,
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


def test_authenticate_basic(requests_mock, api_version):
    user, pwd = "john262", "J0hndo3"
    requests_mock.get(API_URL, json={"api_version": api_version})

    def text_callback(request, context):
        assert request.headers["Authorization"] == requests.auth._basic_auth_str(username=user, password=pwd)
        return '{"access_token":"w3lc0m3"}'

    requests_mock.get(API_URL + 'credentials/basic', text=text_callback)

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
    requests_mock.get(API_URL, json={"api_version": api_version})

    def text_callback(request, context):
        assert request.headers["Authorization"] == requests.auth._basic_auth_str(username=user, password=pwd)
        return '{"access_token":"w3lc0m3"}'

    requests_mock.get(API_URL + 'credentials/basic', text=text_callback)
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
def test_authenticate_oidc_authorization_code_100_single_implicit(requests_mock):
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
        oidc_discovery_url="https://fauth.test/.well-known/openid-configuration",
        scopes_supported=["openid", "im"],
    )

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_oidc_authorization_code(client_id=client_id, webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/fauth/' + oidc_mock.state["access_token"]


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


def test_authenticate_oidc_authorization_code_100_multiple_no_id(requests_mock):
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
    match = r"No OIDC provider id given. Pick one from: \[('fauth', 'bauth'|'bauth', 'fauth')\]\."
    with pytest.raises(OpenEoClientException, match=match):
        conn.authenticate_oidc_authorization_code(client_id=client_id, webbrowser_open=pytest.fail)


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
        oidc_discovery_url="https://bauth.test/.well-known/openid-configuration",
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": expected_scope},
        oidc_discovery_url=oidc_discovery_url,
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid"},
        oidc_discovery_url=oidc_discovery_url,
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret},
        oidc_discovery_url=oidc_discovery_url,
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret},
        oidc_discovery_url=oidc_discovery_url,
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
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
        oidc_discovery_url=oidc_discovery_url,
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
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
        oidc_discovery_url=oidc_discovery_url,
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


@pytest.mark.slow
@pytest.mark.parametrize(
    ["store_refresh_token", "scopes_supported", "expected_scopes"],
    [
        (False, ["openid", "email"], "openid"),
        (False, ["openid", "email", "offline_access"], "openid"),
        (True, ["openid", "email"], "openid"),
        (True, ["openid", "email", "offline_access"], "offline_access openid"),
    ]
)
def test_authenticate_oidc_device_flow(requests_mock, store_refresh_token, scopes_supported, expected_scopes):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
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
        oidc_discovery_url=oidc_discovery_url,
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
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


@pytest.mark.slow
def test_authenticate_oidc_device_flow_client_from_config(requests_mock, auth_config):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
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
        oidc_discovery_url=oidc_discovery_url,
    )
    auth_config.set_oidc_client_config(
        backend=API_URL, provider_id="oi", client_id=client_id, client_secret=client_secret
    )

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    conn.authenticate_oidc_device()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.slow
def test_authenticate_oidc_device_flow_no_support(requests_mock, auth_config):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    client_secret = "$3cr3t"
    issuer = "https://oidc.test"
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_discovery_url=oidc_discovery_url,
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
    with pytest.raises(OidcException, match="No support for device code flow"):
        conn.authenticate_oidc_device()


def test_authenticate_oidc_device_flow_multiple_providers_no_given(requests_mock, auth_config):
    """OIDC device flow with multiple OIDC providers and none specified to use."""
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo Auth", "scopes": ["openid", "w"]},
            {"id": "bauth", "issuer": "https://bauth.test", "title": "Bar Auth", "scopes": ["openid", "w"]},
        ]
    })
    assert auth_config.load() == {}

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    match = r"No OIDC provider id given. Pick one from: \[('fauth', 'bauth'|'bauth', 'fauth')\]\."
    with pytest.raises(OpenEoClientException, match=match):
        conn.authenticate_oidc_device(client_id=client_id)


@pytest.mark.slow
def test_authenticate_oidc_device_flow_multiple_provider_one_config_no_given(requests_mock, auth_config):
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
            "scope": "openid", "code_verifier": True, "code_challenge": True
        },
        scopes_supported=["openid"],
        oidc_discovery_url="https://fauth.test/.well-known/openid-configuration",
    )
    assert auth_config.load() == {}
    auth_config.set_oidc_client_config(backend=API_URL, provider_id="fauth", client_id=client_id)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    conn.authenticate_oidc_device()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/fauth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


@pytest.mark.slow
def test_authenticate_oidc_device_flow_multiple_provider_one_config_no_given_default_client(requests_mock, auth_config):
    """
    OIDC device flow + default_client + PKCE with multiple OIDC providers, one in config and none specified to use.
    """
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    default_client_id = "dadefaultklient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "fauth", "issuer": "https://fauth.test", "title": "Foo", "scopes": ["openid"]},
            {
                "id": "bauth", "issuer": "https://bauth.test", "title": "Bar", "scopes": ["openid"],
                "default_client": {"id": default_client_id}
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
        oidc_discovery_url="https://bauth.test/.well-known/openid-configuration",
    )
    assert auth_config.load() == {}
    auth_config.set_oidc_client_config(backend=API_URL, provider_id="bauth", client_id=None)

    # With all this set up, kick off the openid connect flow
    refresh_token_store = mock.Mock()
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    conn.authenticate_oidc_device()
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/bauth/' + oidc_mock.state["access_token"]
    assert refresh_token_store.mock_calls == []


def test_authenticate_oidc_refresh_token(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    refresh_token = "r3fr35h!"
    issuer = "https://oidc.test"
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
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
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
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
    if store_refresh_token:
        assert new_refresh_token != orig_refresh_token
        assert new_refresh_token == oidc_mock.state["refresh_token"]
    else:
        assert new_refresh_token == orig_refresh_token
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == ["refresh_token"]


@pytest.mark.slow
def test_authenticate_oidc_auto_no_existing_refresh_token(requests_mock, refresh_token_store):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    issuer = "https://oidc.test"
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    # TODO: we're mixing both refresh_token and device_code flow in same mock here
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_client_id=client_id,
        expected_grant_type=None,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={
            "refresh_token": "unkn0wn",
            "scope": "openid",
            "code_verifier": True, "code_challenge": True
        }
    )

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    conn.authenticate_oidc(client_id=client_id)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == [
        "urn:ietf:params:oauth:grant-type:device_code"
    ]


@pytest.mark.slow
def test_authenticate_oidc_auto_expired_refresh_token(requests_mock, refresh_token_store):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    issuer = "https://oidc.test"
    oidc_discovery_url = "https://oidc.test/.well-known/openid-configuration"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "oi", "issuer": issuer, "title": "example", "scopes": ["openid"]}]
    })
    # TODO: we're mixing both refresh_token and device_code flow in same mock here
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_client_id=client_id,
        expected_grant_type=None,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={
            "refresh_token": "unkn0wn",
            "scope": "openid",
            "code_verifier": True, "code_challenge": True
        }
    )
    refresh_token_store.set_refresh_token(issuer=issuer, client_id=client_id, refresh_token="0ld.t0k3n")

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL, refresh_token_store=refresh_token_store)
    assert isinstance(conn.auth, NullAuth)
    oidc_mock.state["device_code_callback_timeline"] = ["great success"]
    conn.authenticate_oidc(client_id=client_id)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/oi/' + oidc_mock.state["access_token"]
    assert [r["grant_type"] for r in oidc_mock.grant_request_history] == [
        "refresh_token",
        "urn:ietf:params:oauth:grant-type:device_code",
    ]
    assert oidc_mock.grant_request_history[0]["response"] == '{"error": "invalid refresh token"}'


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
    node = im.graph[im.node_id]
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


def test_list_file_formats(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    file_formats = {
        "input": {"GeoJSON": {"gis_data_type": ["vector"]}},
        "output": {"GTiff": {"gis_data_types": ["raster"]}},
    }
    requests_mock.get(API_URL + "file_formats", json=file_formats)
    assert conn.list_file_formats() == file_formats


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
        mock.call("post", path="/result", json={"process_graph": {"foo1": {"process_id": "foo"}}})
    ]


def test_execute_100(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    conn = Connection(API_URL)
    with mock.patch.object(conn, "request") as request:
        conn.execute({"foo1": {"process_id": "foo"}})
    assert request.call_args_list == [
        mock.call("post", path="/result", json={"process": {"process_graph": {"foo1": {"process_id": "foo"}}}})
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
    tmp_path = Path(str(tmp_path))  # Python 3.5 workaround

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

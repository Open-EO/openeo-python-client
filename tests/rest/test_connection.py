import re
import unittest.mock as mock

import pytest
import requests_mock

from openeo.capabilities import ComparableVersion
from openeo.rest import OpenEoClientException
from openeo.rest.auth.auth import NullAuth, BearerAuth
from openeo.rest.connection import Connection, RestApiConnection, connect, OpenEoApiError
from .auth.test_oidc import OidcMock

API_URL = "https://oeo.net/"


@pytest.mark.parametrize(
    ["base", "paths", "expected_path"],
    [
        # Simple
        ("https://oeo.net", ["foo", "/foo"], "https://oeo.net/foo"),
        ("https://oeo.net/", ["foo", "/foo"], "https://oeo.net/foo"),
        # With trailing slash
        ("https://oeo.net", ["foo/", "/foo/"], "https://oeo.net/foo/"),
        ("https://oeo.net/", ["foo/", "/foo/"], "https://oeo.net/foo/"),
        # Deeper
        ("https://oeo.net/api/v04", ["foo/bar", "/foo/bar"], "https://oeo.net/api/v04/foo/bar"),
        ("https://oeo.net/api/v04/", ["foo/bar", "/foo/bar"], "https://oeo.net/api/v04/foo/bar"),
        ("https://oeo.net/api/v04", ["foo/bar/", "/foo/bar/"], "https://oeo.net/api/v04/foo/bar/"),
        ("https://oeo.net/api/v04/", ["foo/bar/", "/foo/bar/"], "https://oeo.net/api/v04/foo/bar/"),
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
    requests_mock.get("https://oeo.net/foo", status_code=200, json={"o": "k"})
    # Expected status
    assert conn.get("/foo", expected_status=200).json() == {"o": "k"}
    assert conn.get("/foo", expected_status=[200, 201]).json() == {"o": "k"}
    # Unexpected status
    with pytest.raises(OpenEoClientException, match=r"Got status code 200 for `GET /foo` \(expected 204\)"):
        conn.get("/foo", expected_status=204)
    with pytest.raises(OpenEoClientException, match=r"Got status code 200 for `GET /foo` \(expected \[203, 204\]\)"):
        conn.get("/foo", expected_status=[203, 204])


def test_rest_api_expected_status_with_error(requests_mock):
    conn = RestApiConnection(API_URL)
    requests_mock.get("https://oeo.net/bar", status_code=406, json={"code": "NoBar", "message": "no bar please"})
    # First check for API error by default
    with pytest.raises(OpenEoApiError, match=r"\[406\] NoBar: no bar please"):
        conn.get("/bar", expected_status=200)
    with pytest.raises(OpenEoApiError, match=r"\[406\] NoBar: no bar please"):
        conn.get("/bar", expected_status=[201, 202])
    # Don't check for error, just status
    conn.get("/bar", check_error=False, expected_status=406)
    with pytest.raises(OpenEoClientException, match=r"Got status code 406 for `GET /bar` \(expected 302\)"):
        conn.get("/bar", check_error=False, expected_status=302)
    with pytest.raises(OpenEoClientException, match=r"Got status code 406 for `GET /bar` \(expected \[302, 303\]\)"):
        conn.get("/bar", check_error=False, expected_status=[302, 303])


def test_502_proxy_error(requests_mock):
    """EP-3387"""
    requests_mock.get("https://oeo.net/bar", status_code=502, text="""<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">
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

def test_connection_with_session():
    session = mock.Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {"foo": "bar", "api_version": "0.4.0"}
    conn = Connection("https://oeo.net/", session=session)
    assert conn.capabilities().capabilities["foo"] == "bar"
    session.request.assert_any_call(
        url="https://oeo.net/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY, timeout=None
    )


def test_connect_with_session():
    session = mock.Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {"foo": "bar", "api_version": "0.4.0"}
    conn = connect("https://oeo.net/", session=session)
    assert conn.capabilities().capabilities["foo"] == "bar"
    session.request.assert_any_call(
        url="https://oeo.net/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY, timeout=None
    )


@pytest.mark.parametrize(["versions", "expected_url", "expected_version"], [
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.net/openeo/0.4.1/"},
                {"api_version": "0.4.2", "url": "https://oeo.net/openeo/0.4.2/"},
                {"api_version": "1.0.0", "url": "https://oeo.net/openeo/1.0.0/"},
            ],
            "https://oeo.net/openeo/1.0.0/",
            "1.0.0",
    ),
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.net/openeo/0.4.1/"},
                {"api_version": "0.4.2", "url": "https://oeo.net/openeo/0.4.2/"},
                {"api_version": "1.0.0", "url": "https://oeo.net/openeo/1.0.0/", "production": False},
            ],
            "https://oeo.net/openeo/0.4.2/",
            "0.4.2",
    ),
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.net/openeo/0.4.1/", "production": True},
                {"api_version": "0.4.2", "url": "https://oeo.net/openeo/0.4.2/", "production": True},
                {"api_version": "1.0.0", "url": "https://oeo.net/openeo/1.0.0/", "production": False},
            ],
            "https://oeo.net/openeo/0.4.2/",
            "0.4.2",
    ),
    (
            [
                {"api_version": "0.4.1", "url": "https://oeo.net/openeo/0.4.1/", "production": False},
                {"api_version": "0.4.2", "url": "https://oeo.net/openeo/0.4.2/", "production": False},
                {"api_version": "1.0.0", "url": "https://oeo.net/openeo/1.0.0/", "production": False},
            ],
            "https://oeo.net/openeo/1.0.0/",
            "1.0.0",
    ),
    (
            [
                {"api_version": "0.1.0", "url": "https://oeo.net/openeo/0.1.0/", "production": True},
                {"api_version": "0.4.2", "url": "https://oeo.net/openeo/0.4.2/", "production": False},
            ],
            "https://oeo.net/openeo/0.4.2/",
            "0.4.2",
    ),

])
def test_connect_version_discovery(requests_mock, versions, expected_url, expected_version):
    requests_mock.get("https://oeo.net/", status_code=404)
    requests_mock.get("https://oeo.net/.well-known/openeo", status_code=200, json={"versions": versions})
    requests_mock.get(expected_url, status_code=200, json={"foo": "bar", "api_version": expected_version})

    conn = connect("https://oeo.net/")
    assert conn.capabilities().capabilities["foo"] == "bar"


def test_api_error(requests_mock):
    requests_mock.get('https://oeo.net/', json={"api_version": "0.4.0"})
    conn = Connection(API_URL)
    requests_mock.get('https://oeo.net/collections/foobar', status_code=404, json={
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
    requests_mock.get('https://oeo.net/', json={"api_version": "0.4.0"})
    conn = Connection(API_URL)
    requests_mock.get('https://oeo.net/collections/foobar', status_code=500, text="olapola")
    with pytest.raises(OpenEoApiError) as exc_info:
        conn.describe_collection("foobar")
    exc = exc_info.value
    assert exc.http_status_code == 500
    assert exc.code == "unknown"
    assert exc.message == "olapola"
    assert exc.id is None
    assert exc.url is None


def test_authenticate_basic(requests_mock, api_version):
    requests_mock.get(API_URL, json={"api_version": api_version})
    conn = Connection(API_URL)

    def text_callback(request, context):
        assert request.headers["Authorization"] == "Basic am9objpqMGhu"
        return '{"access_token":"w3lc0m3"}'

    requests_mock.get(API_URL + 'credentials/basic', text=text_callback)

    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_basic(username="john", password="j0hn")
    assert isinstance(conn.auth, BearerAuth)
    if ComparableVersion(api_version).at_least("1.0.0"):
        assert conn.auth.bearer == "basic//w3lc0m3"
    else:
        assert conn.auth.bearer == "w3lc0m3"


def test_authenticate_oidc_040(requests_mock):
    client_id = "myclient"
    oidc_discovery_url = "https://oeo.net/credentials/oidc"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url
    )
    requests_mock.get(API_URL, json={"api_version": "0.4.0"})

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_OIDC(client_id=client_id, webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == oidc_mock.state["access_token"]


def test_authenticate_oidc_100_single_implicit(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "foidc", "issuer": "https://auth.foidc.net", "title": "FOIDC"}]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        oidc_discovery_url="https://auth.foidc.net/.well-known/openid-configuration"
    )

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_OIDC(client_id=client_id, webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/foidc/' + oidc_mock.state["access_token"]


def test_authenticate_oidc_100_single_wrong_id(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [{"id": "foidc", "issuer": "https://auth.foidc.net", "title": "FOIDC"}]
    })

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    with pytest.raises(OpenEoClientException, match=r"'nopenope' not available\. Should be one of \['foidc'\]\."):
        conn.authenticate_OIDC(client_id=client_id, provider_id="nopenope", webbrowser_open=pytest.fail)


def test_authenticate_oidc_100_multiple_no_id(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "foidc", "issuer": "https://auth.foidc.net", "title": "FOIDC"},
            {"id": "baroi", "issuer": "https://acco.baroi.net", "title": "BarOI"},
        ]
    })

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    match = r"No provider_id given. Available: \[('foidc', 'baroi'|'baroi', 'foidc')\]\."
    with pytest.raises(OpenEoClientException, match=match):
        conn.authenticate_OIDC(client_id=client_id, webbrowser_open=pytest.fail)


def test_authenticate_oidc_100_multiple_wrong_id(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "foidc", "issuer": "https://auth.foidc.net", "title": "FOIDC"},
            {"id": "baroi", "issuer": "https://acco.baroi.net", "title": "BarOI"},
        ]
    })

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    match = r"'lol' not available\. Should be one of \[('foidc', 'baroi'|'baroi', 'foidc')\]\."
    with pytest.raises(OpenEoClientException, match=match):
        conn.authenticate_OIDC(client_id=client_id, provider_id="lol", webbrowser_open=pytest.fail)


def test_authenticate_oidc_100_multiple_success(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "1.0.0"})
    client_id = "myclient"
    requests_mock.get(API_URL + 'credentials/oidc', json={
        "providers": [
            {"id": "foidc", "issuer": "https://auth.foidc.net", "title": "FOIDC"},
            {"id": "baroi", "issuer": "https://acco.baroi.net", "title": "BarOI"},
        ]
    })
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        oidc_discovery_url="https://acco.baroi.net/.well-known/openid-configuration"
    )

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_OIDC(client_id=client_id, provider_id="baroi", webbrowser_open=oidc_mock.webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == 'oidc/baroi/' + oidc_mock.state["access_token"]


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

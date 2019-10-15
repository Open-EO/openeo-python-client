import unittest.mock as mock

import pytest
import requests_mock

from openeo.rest.auth.auth import NullAuth, BearerAuth
from openeo.rest.connection import Connection, RestApiConnection, connect, OpenEoApiError

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
            assert request.headers["User-Agent"].startswith("openeo-python-client")
            assert request.headers["X-Openeo-Bar"] == "XY123"

        m.get("/foo", text=text)
        m.post("/foo", text=text)
        conn.get("/foo", headers={"X-Openeo-Bar": "XY123"})
        conn.post("/foo", {}, headers={"X-Openeo-Bar": "XY123"})


def test_connection_with_session():
    session = mock.Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {"foo": "bar", "api_version": "0.4.0"}
    conn = Connection("https://oeo.net/", session=session)
    assert conn.capabilities().capabilities["foo"] == "bar"
    session.request.assert_any_call(
        url="https://oeo.net/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY
    )


def test_connect_with_session():
    session = mock.Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {"foo": "bar", "api_version": "0.4.0"}
    conn = connect("https://oeo.net/", session=session)
    assert conn.capabilities().capabilities["foo"] == "bar"
    session.request.assert_any_call(
        url="https://oeo.net/", method="get", headers=mock.ANY, stream=mock.ANY, auth=mock.ANY
    )


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


def test_authenticate_basic(requests_mock):
    requests_mock.get(API_URL, json={"api_version": "0.4.0"})
    conn = Connection(API_URL)

    def text_callback(request, context):
        assert request.headers["Authorization"] == "Basic am9objpqMGhu"
        return '{"access_token":"w3lc0m3"}'

    requests_mock.get(API_URL + 'credentials/basic', text=text_callback)

    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_basic(username="john", password="j0hn")
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == "w3lc0m3"


def test_authenticate_oidc(oidc_test_setup, requests_mock):
    # see test/rest/conftest.py for `oidc_test_setup` fixture
    client_id = "myclient"
    oidc_discovery_url = "https://oeo.net/credentials/oidc"
    state, webbrowser_open = oidc_test_setup(client_id=client_id, oidc_discovery_url=oidc_discovery_url)
    requests_mock.get(API_URL, json={"api_version": "0.4.0"})

    # With all this set up, kick off the openid connect flow
    conn = Connection(API_URL)
    assert isinstance(conn.auth, NullAuth)
    conn.authenticate_OIDC(client_id=client_id, webbrowser_open=webbrowser_open)
    assert isinstance(conn.auth, BearerAuth)
    assert conn.auth.bearer == state["access_token"]


def test_load_collection_arguments(requests_mock):
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

import base64
import contextlib
import json
import logging
import re
import urllib.parse
import urllib.parse
from io import BytesIO
from queue import Queue
from typing import List, Union, Optional
from unittest import mock
import uuid

import pytest
import requests
import requests_mock.request

import openeo.rest.auth.oidc
from openeo.rest.auth.oidc import QueuingRequestHandler, drain_queue, HttpServerThread, OidcAuthCodePkceAuthenticator, \
    OidcClientCredentialsAuthenticator, OidcResourceOwnerPasswordAuthenticator, OidcClientInfo, OidcProviderInfo, \
    OidcDeviceAuthenticator, random_string, OidcRefreshTokenAuthenticator, PkceCode, OidcException, \
    DefaultOidcClientGrant
from openeo.util import dict_no_none

DEVICE_CODE_POLL_INTERVAL = 2

# Sentinel object to indicate that a field should be absent.
ABSENT = object()


def handle_request(handler_class, path: str):
    """
    Fake (serverless) request handling

    :param handler_class: should be a subclass of `http.server.BaseHTTPRequestHandler`
    """

    class Request:
        """Fake socket-like request object."""

        def __init__(self):
            # Pass the requested URL path in HTTP format.
            self.rfile = BytesIO("GET {p} HTTP/1.1\r\n".format(p=path).encode('utf-8'))
            self.wfile = BytesIO()

        def makefile(self, mode, *args, **kwargs):
            return {'rb': self.rfile, 'wb': self.wfile}[mode]

        def sendall(self, bytes):
            self.wfile.write(bytes)

    request = Request()
    handler_class(request=request, client_address=('0.0.0.0', 8910), server=None)


def test_queuing_request_handler():
    queue = Queue()
    handle_request(QueuingRequestHandler.with_queue(queue), path="/foo/bar")
    assert list(drain_queue(queue)) == ['/foo/bar']


@pytest.mark.slow
def test_http_server_thread():
    queue = Queue()
    server_thread = HttpServerThread(RequestHandlerClass=QueuingRequestHandler.with_queue(queue))
    server_thread.start()
    port, host, fqdn = server_thread.server_address_info()
    if host == '0.0.0.0':
        host = '127.0.0.1'
    url = 'http://{h}:{p}/foo/bar'.format(h=host, p=port)
    response = requests.get(url)
    response.raise_for_status()
    assert list(drain_queue(queue)) == ['/foo/bar']
    server_thread.shutdown()
    server_thread.join()


@pytest.mark.slow
def test_http_server_thread_port():
    queue = Queue()
    server_thread = HttpServerThread(RequestHandlerClass=QueuingRequestHandler.with_queue(queue),
                                     server_address=('', 12345))
    server_thread.start()
    port, host, fqdn = server_thread.server_address_info()
    if host == '0.0.0.0':
        host = '127.0.0.1'
    assert port == 12345
    url = 'http://{h}:{p}/foo/bar'.format(h=host, p=port)
    response = requests.get(url)
    response.raise_for_status()
    assert list(drain_queue(queue)) == ['/foo/bar']
    server_thread.shutdown()
    server_thread.join()


def test_provider_info_issuer(requests_mock):
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"scopes_supported": ["openid"]})
    p = OidcProviderInfo(issuer="https://authit.test")
    assert p.discovery_url == "https://authit.test/.well-known/openid-configuration"
    assert p.get_scopes_string() == "openid"


def test_provider_info_issuer_slash(requests_mock):
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"scopes_supported": ["openid"]})
    p = OidcProviderInfo(issuer="https://authit.test/")
    assert p.discovery_url == "https://authit.test/.well-known/openid-configuration"


def test_provider_info_discovery_url(requests_mock):
    discovery_url = "https://authit.test/.well-known/openid-configuration"
    requests_mock.get(discovery_url, json={"issuer": "https://authit.test"})
    p = OidcProviderInfo(discovery_url=discovery_url)
    assert p.discovery_url == "https://authit.test/.well-known/openid-configuration"
    assert p.get_scopes_string() == "openid"


def test_provider_info_scopes(requests_mock):
    requests_mock.get(
        "https://authit.test/.well-known/openid-configuration",
        json={"scopes_supported": ["openid", "test"]}
    )
    assert "openid" == OidcProviderInfo(issuer="https://authit.test").get_scopes_string()
    assert "openid" == OidcProviderInfo(issuer="https://authit.test", scopes=[]).get_scopes_string()
    assert "openid test" == OidcProviderInfo(issuer="https://authit.test", scopes=["test"]).get_scopes_string()
    assert "openid test" == OidcProviderInfo(
        issuer="https://authit.test", scopes=["openid", "test"]
    ).get_scopes_string()
    assert "openid test" == OidcProviderInfo(
        issuer="https://authit.test", scopes=("openid", "test")
    ).get_scopes_string()
    assert "openid test" == OidcProviderInfo(
        issuer="https://authit.test", scopes={"openid", "test"}
    ).get_scopes_string()


def test_provider_info_default_client_none(requests_mock):
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={})
    info = OidcProviderInfo(issuer="https://authit.test")
    assert info.get_default_client_id(grant_check=[]) is None
    assert info.get_default_client_id(grant_check=lambda grants: True) is None


def test_provider_info_default_client_available_list(requests_mock):
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={})
    default_clients = [{
        "id": "jak4l0v3-45lsdfe3d",
        "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
    }]
    info = OidcProviderInfo(issuer="https://authit.test", default_clients=default_clients)

    # Alias for compactness
    g = DefaultOidcClientGrant

    assert info.get_default_client_id(grant_check=[]) == "jak4l0v3-45lsdfe3d"
    assert info.get_default_client_id(grant_check=[g.DEVICE_CODE_PKCE]) == "jak4l0v3-45lsdfe3d"
    assert info.get_default_client_id(grant_check=[g.REFRESH_TOKEN]) == "jak4l0v3-45lsdfe3d"
    assert info.get_default_client_id(grant_check=[g.DEVICE_CODE_PKCE, g.REFRESH_TOKEN]) == "jak4l0v3-45lsdfe3d"

    assert info.get_default_client_id(grant_check=[g.IMPLICIT]) is None
    assert info.get_default_client_id(grant_check=[g.IMPLICIT, g.REFRESH_TOKEN]) is None


def test_provider_info_default_client_available_lambda(requests_mock):
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={})
    default_clients = [{
        "id": "jak4l0v3-45lsdfe3d",
        "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
    }]
    info = OidcProviderInfo(issuer="https://authit.test", default_clients=default_clients)

    # Alias for compactness
    g = DefaultOidcClientGrant

    assert info.get_default_client_id(grant_check=lambda grants: True) == "jak4l0v3-45lsdfe3d"
    assert info.get_default_client_id(grant_check=lambda grants: g.REFRESH_TOKEN in grants) == "jak4l0v3-45lsdfe3d"
    assert info.get_default_client_id(grant_check=lambda grants: g.DEVICE_CODE_PKCE in grants) == "jak4l0v3-45lsdfe3d"
    assert info.get_default_client_id(
        grant_check=lambda grants: g.DEVICE_CODE_PKCE in grants and g.REFRESH_TOKEN in grants
    ) == "jak4l0v3-45lsdfe3d"

    assert info.get_default_client_id(grant_check=lambda grants: False) is None
    assert info.get_default_client_id(grant_check=lambda grants: g.IMPLICIT in grants) is None
    assert info.get_default_client_id(
        grant_check=lambda grants: g.IMPLICIT in grants and g.REFRESH_TOKEN in grants
    ) is None

    assert info.get_default_client_id(
        grant_check=lambda grants: g.IMPLICIT in grants or g.REFRESH_TOKEN in grants
    ) == "jak4l0v3-45lsdfe3d"


def test_provider_info_default_client_invalid_grants(requests_mock, caplog):
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={})
    default_clients = [{
        "id": "jak4l0v3-45lsdfe3d",
        "grant_types": ["refresh_token", "nope dis invalid"]
    }]
    info = OidcProviderInfo(issuer="https://authit.test", default_clients=default_clients)

    # Alias for compactness
    g = DefaultOidcClientGrant

    with caplog.at_level(logging.WARNING):
        assert info.get_default_client_id(grant_check=[g.REFRESH_TOKEN]) == "jak4l0v3-45lsdfe3d"
    assert "Invalid OIDC grant type 'nope dis" in caplog.text


@pytest.mark.parametrize(
    ["scopes_supported", "expected"], [
        (["openid", "email"], "openid"),
        (["openid", "email", "offline_access"], "offline_access openid"),
    ])
def test_provider_info_get_scopes_string_refresh_token_offline_access(requests_mock, scopes_supported, expected):
    requests_mock.get(
        "https://authit.test/.well-known/openid-configuration",
        json={"scopes_supported": scopes_supported}
    )
    p = OidcProviderInfo(issuer="https://authit.test")
    assert p.get_scopes_string() == "openid"
    assert p.get_scopes_string(request_refresh_token=True) == expected
    assert p.get_scopes_string() == "openid"


def test_oidc_client_info_uess_device_flow_pkce_support(requests_mock):
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_discovery_url=oidc_discovery_url,
        expected_grant_type=None,
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url, default_clients=[
        {"id": "c1", "grant_types": ["authorization_code+pkce"]},
        {"id": "c2", "grant_types": ["urn:ietf:params:oauth:grant-type:device_code"]},
        {"id": "c3", "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce"]},
        {"id": "c4", "grant_types": ["refresh_token", "urn:ietf:params:oauth:grant-type:device_code+pkce"]},
    ])

    for client_id, expected in [
        ("c1", False),
        ("c2", False),
        ("c3", True),
        ("c4", True),
        ("foo", False)
    ]:
        client_info = OidcClientInfo(client_id=client_id, provider=provider)
        assert client_info.guess_device_flow_pkce_support() is expected


class OidcMock:
    """
    Mock object to test OIDC flows
    """

    def __init__(
            self,
            requests_mock: requests_mock.Mocker,
            oidc_discovery_url: str,
            expected_grant_type: Union[str, None],
            expected_client_id: str = "myclient",
            expected_fields: dict = None,
            provider_root_url: str = "https://auth.test",
            state: dict = None,
            scopes_supported: List[str] = None,
            device_code_flow_support: bool = True,
    ):
        self.requests_mock = requests_mock
        self.oidc_discovery_url = oidc_discovery_url
        self.expected_grant_type = expected_grant_type
        self.grant_request_history = []
        self.expected_client_id = expected_client_id
        self.expected_fields = expected_fields or {}
        self.expected_authorization_code = None
        self.provider_root_url = provider_root_url
        self.authorization_endpoint = provider_root_url + "/auth"
        self.token_endpoint = provider_root_url + "/token"
        self.device_code_endpoint = provider_root_url + "/device_code" if device_code_flow_support else None
        self.state = state or {}
        self.scopes_supported = scopes_supported or ["openid", "email", "profile"]

        self.requests_mock.get(oidc_discovery_url, text=json.dumps(dict_no_none({
            # Rudimentary OpenID Connect discovery document
            "issuer": self.provider_root_url,
            "authorization_endpoint": self.authorization_endpoint,
            "token_endpoint": self.token_endpoint,
            "device_authorization_endpoint": self.device_code_endpoint,
            "scopes_supported": self.scopes_supported
        })))
        self.requests_mock.post(self.token_endpoint, text=self.token_callback)

        if self.device_code_endpoint:
            self.requests_mock.post(
                self.device_code_endpoint,
                text=self.device_code_callback
            )

    def webbrowser_open(self, url: str):
        """Doing fake browser and Oauth Provider handling here"""
        assert url.startswith(self.authorization_endpoint)
        params = self._get_query_params(url=url)
        assert params["client_id"] == self.expected_client_id
        assert params["response_type"] == "code"
        assert params["scope"] == self.expected_fields["scope"]
        for key in ["state", "nonce", "code_challenge", "redirect_uri", "scope"]:
            self.state[key] = params[key]
        redirect_uri = params["redirect_uri"]
        # Don't mock the request to the redirect URI (it is hosted by the temporary web server in separate thread)
        self.requests_mock.get(redirect_uri, real_http=True)
        self.expected_authorization_code = "6uthc0d3"
        requests.get(redirect_uri, params={"state": params["state"], "code": self.expected_authorization_code})

    def token_callback(self, request: requests_mock.request._RequestObjectProxy, context):
        params = self._get_query_params(query=request.text)
        grant_type = params["grant_type"]
        self.grant_request_history.append({"grant_type": grant_type})
        if self.expected_grant_type:
            assert grant_type == self.expected_grant_type
        callback = {
            "authorization_code": self.token_callback_authorization_code,
            "client_credentials": self.token_callback_client_credentials,
            "password": self.token_callback_resource_owner_password_credentials,
            "urn:ietf:params:oauth:grant-type:device_code": self.token_callback_device_code,
            "refresh_token": self.token_callback_refresh_token,
        }[grant_type]
        result = callback(params=params, context=context)
        try:
            result_decoded = json.loads(result)
            self.grant_request_history[-1]["response"] = result_decoded
        except json.JSONDecodeError:
            self.grant_request_history[-1]["response"] = result
        return result

    def token_callback_authorization_code(self, params: dict, context):
        """Fake code to token exchange by Oauth Provider"""
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "authorization_code"
        assert self.state["code_challenge"] == PkceCode.sha256_hash(params["code_verifier"])
        assert params["code"] == self.expected_authorization_code
        assert params["redirect_uri"] == self.state["redirect_uri"]
        return self._build_token_response()

    def token_callback_client_credentials(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "client_credentials"
        assert params["client_secret"] == self.expected_fields["client_secret"]
        return self._build_token_response(include_id_token=False)

    def token_callback_resource_owner_password_credentials(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "password"
        assert params["client_secret"] == self.expected_fields["client_secret"]
        assert params["username"] == self.expected_fields["username"]
        assert params["password"] == self.expected_fields["password"]
        assert params["scope"] == self.expected_fields["scope"]
        return self._build_token_response()

    def device_code_callback(self, request: requests_mock.request._RequestObjectProxy, context):
        params = self._get_query_params(query=request.text)
        assert params["client_id"] == self.expected_client_id
        assert params["scope"] == self.expected_fields["scope"]
        self.state["device_code"] = random_string()
        self.state["user_code"] = random_string(length=6).upper()
        self.state["scope"] = params["scope"]
        if "code_challenge" in self.expected_fields:
            expect_code_challenge = self.expected_fields.get("code_challenge")
            if expect_code_challenge in [True]:
                assert "code_challenge" in params
                self.state["code_challenge"] = params["code_challenge"]
            elif expect_code_challenge in [False, ABSENT]:
                assert "code_challenge" not in params
            else:
                raise ValueError(expect_code_challenge)
        return json.dumps({
            # TODO: also verification_url (google tweak)
            "verification_uri": self.provider_root_url + "/dc",
            "device_code": self.state["device_code"],
            "user_code": self.state["user_code"],
            "interval": DEVICE_CODE_POLL_INTERVAL,
        })

    def token_callback_device_code(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        expected_client_secret = self.expected_fields.get("client_secret")
        if expected_client_secret:
            assert params["client_secret"] == expected_client_secret
        else:
            assert "client_secret" not in params
        expect_code_verifier = self.expected_fields.get("code_verifier")
        if expect_code_verifier in [True]:
            assert PkceCode.sha256_hash(params["code_verifier"]) == self.state["code_challenge"]
            self.state["code_verifier"] = params["code_verifier"]
        elif expect_code_verifier in [False, None, ABSENT]:
            assert "code_verifier" not in params
            assert "code_challenge" not in self.state
        else:
            raise ValueError(expect_code_verifier)
        assert params["device_code"] == self.state["device_code"]
        assert params["grant_type"] == "urn:ietf:params:oauth:grant-type:device_code"
        # Fail with pending/too fast?
        try:
            result = self.state["device_code_callback_timeline"].pop(0)
        except Exception:
            result = "rest in peace"
        if result == "great success":
            return self._build_token_response()
        else:
            context.status_code = 400
            return json.dumps({"error": result})

    def token_callback_refresh_token(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "refresh_token"
        if "client_secret" in self.expected_fields:
            assert params["client_secret"] == self.expected_fields["client_secret"]
        if params["refresh_token"] != self.expected_fields["refresh_token"]:
            context.status_code = 401
            return json.dumps({"error": "invalid refresh token"})
        assert params["refresh_token"] == self.expected_fields["refresh_token"]
        return self._build_token_response(include_id_token=False, include_refresh_token=False)

    @staticmethod
    def _get_query_params(*, url=None, query=None):
        """Helper to extract query params from an url or query string"""
        if not query:
            query = urllib.parse.urlparse(url).query
        params = {}
        for param, values in urllib.parse.parse_qs(query).items():
            assert len(values) == 1
            params[param] = values[0]
        return params

    @staticmethod
    def _jwt_encode(header: dict, payload: dict, signature="s1gn6tur3"):
        """Poor man's JWT encoding (just for unit testing purposes)"""

        def encode(d):
            return base64.urlsafe_b64encode(json.dumps(d).encode("ascii")).decode("ascii").replace('=', '')

        return ".".join([encode(header), encode(payload), signature])

    def _build_token_response(
            self, sub="123", name="john", include_id_token=True, include_refresh_token: Optional[bool] = None
    ) -> str:
        """Build JSON serialized access/id/refresh token response (and store tokens for use in assertions)"""
        access_token = self._jwt_encode(
            header={},
            payload=dict_no_none(sub=sub, name=name, nonce=self.state.get("nonce"), _uuid=uuid.uuid4().hex),
        )
        res = {"access_token": access_token}

        # Attempt to simulate real world refresh token support.
        if include_refresh_token is None:
            if "offline_access" in self.scopes_supported:
                # "offline_access" scope as suggested in spec
                # (https://openid.net/specs/openid-connect-core-1_0.html#OfflineAccess)
                # Implemented by Microsoft, EGI Check-in
                include_refresh_token = "offline_access" in self.state.get("scope", "").split(" ")
            else:
                # Google OAuth style: no support for "offline_access", return refresh token automatically?
                include_refresh_token = True
        if include_refresh_token:
            res["refresh_token"] = self._jwt_encode(header={}, payload={"foo": "refresh", "_uuid": uuid.uuid4().hex})
        if include_id_token:
            res["id_token"] = access_token
        self.state.update(res)
        self.state.update(name=name, sub=sub)
        return json.dumps(res)

    def validate_access_token(self, access_token: str):
        if access_token == self.state["access_token"]:
            return {"user_id": self.state["name"], "sub": self.state["sub"]}
        raise LookupError("Invalid access token")

    def invalidate_access_token(self):
        self.state["access_token"] = "***invalidated***"


@contextlib.contextmanager
def assert_device_code_poll_sleep(expect_called=True):
    """Fake sleeping, but check it was called with poll interval (or not)."""
    with mock.patch("time.sleep") as sleep:
        yield
    if expect_called:
        sleep.assert_called_with(DEVICE_CODE_POLL_INTERVAL)
    else:
        sleep.assert_not_called()


@pytest.mark.slow
def test_oidc_auth_code_pkce_flow(requests_mock):
    requests_mock.get("http://oidc.test/.well-known/openid-configuration", json={"scopes_supported": ["openid"]})

    client_id = "myclient"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid testpkce"},
        oidc_discovery_url=oidc_discovery_url,
        scopes_supported=["openid", "testpkce"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url, scopes=["openid", "testpkce"])
    authenticator = OidcAuthCodePkceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        webbrowser_open=oidc_mock.webbrowser_open
    )
    # Do the Oauth/OpenID Connect flow
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_client_credentials_flow(requests_mock):
    client_id = "myclient"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    client_secret = "$3cr3t"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret},
        oidc_discovery_url=oidc_discovery_url
    )

    provider = OidcProviderInfo(discovery_url=oidc_discovery_url)
    authenticator = OidcClientCredentialsAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret)
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_resource_owner_password_credentials_flow(requests_mock):
    client_id = "myclient"
    client_secret = "$3cr3t"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    username, password = "john", "j0hn"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="password",
        expected_client_id=client_id,
        expected_fields={
            "username": username, "password": password, "scope": "openid testpwd", "client_secret": client_secret
        },
        oidc_discovery_url=oidc_discovery_url,
        scopes_supported=["openid", "testpwd"],
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url, scopes=["testpwd"])

    authenticator = OidcResourceOwnerPasswordAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        username=username, password=password,
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_device_flow_with_client_secret(requests_mock, caplog):
    client_id = "myclient"
    client_secret = "$3cr3t"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={"scope": "df openid", "client_secret": client_secret},
        state={"device_code_callback_timeline": ["authorization_pending", "slow_down", "great success"]},
        scopes_supported=["openid", "df"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url, scopes=["df"])
    display = []
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        display=display.append
    )
    with mock.patch.object(openeo.rest.auth.oidc.time, "sleep") as sleep:
        with caplog.at_level(logging.INFO):
            tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token
    assert re.search(
        r"visit https://auth\.test/dc and enter the user code {c!r}".format(c=oidc_mock.state['user_code']),
        display[0]
    )
    assert display[1] == "Authorized successfully."
    assert sleep.mock_calls == [mock.call(2), mock.call(2), mock.call(7)]
    assert re.search(
        r"Authorization pending\..*Polling too fast, will slow down\..*Authorized successfully\.",
        caplog.text,
        flags=re.DOTALL
    )


def test_oidc_device_flow_with_pkce(requests_mock, caplog):
    client_id = "myclient"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={"scope": "df openid", "code_challenge": True, "code_verifier": True},
        state={"device_code_callback_timeline": ["authorization_pending", "slow_down", "great success"]},
        scopes_supported=["openid", "df"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url, scopes=["df"])
    display = []
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        display=display.append,
        use_pkce=True
    )
    with mock.patch.object(openeo.rest.auth.oidc.time, "sleep") as sleep:
        with caplog.at_level(logging.INFO):
            tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token
    assert re.search(
        r"visit https://auth\.test/dc and enter the user code {c!r}".format(c=oidc_mock.state['user_code']),
        display[0]
    )
    assert display[1] == "Authorized successfully."
    assert sleep.mock_calls == [mock.call(2), mock.call(2), mock.call(7)]
    assert re.search(
        r"Authorization pending\..*Polling too fast, will slow down\..*Authorized successfully\.",
        caplog.text,
        flags=re.DOTALL
    )


def test_oidc_device_flow_without_pkce_nor_secret(requests_mock, caplog):
    client_id = "myclient"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={"scope": "df openid", "code_challenge": ABSENT, "code_verifier": ABSENT},
        state={"device_code_callback_timeline": ["authorization_pending", "slow_down", "great success"]},
        scopes_supported=["openid", "df"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url, scopes=["df"])
    display = []
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        display=display.append,
    )
    with mock.patch.object(openeo.rest.auth.oidc.time, "sleep") as sleep:
        with caplog.at_level(logging.INFO):
            tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token
    assert re.search(
        r"visit https://auth\.test/dc and enter the user code {c!r}".format(c=oidc_mock.state['user_code']),
        display[0]
    )
    assert display[1] == "Authorized successfully."
    assert sleep.mock_calls == [mock.call(2), mock.call(2), mock.call(7)]
    assert re.search(
        r"Authorization pending\..*Polling too fast, will slow down\..*Authorized successfully\.",
        caplog.text,
        flags=re.DOTALL
    )


@pytest.mark.parametrize(["mode", "client_id", "use_pkce", "client_secret", "expected_fields"], [
    (
            "client_secret, no PKCE",
            "myclient", False, "$3cr3t",
            {"scope": "df openid", "client_secret": "$3cr3t", "code_challenge": ABSENT, "code_verifier": ABSENT}
    ),
    (
            "client_secret, auto PKCE",
            "myclient", None, "$3cr3t",
            {"scope": "df openid", "client_secret": "$3cr3t", "code_challenge": ABSENT, "code_verifier": ABSENT}
    ),
    (
            "use PKCE",
            "myclient", True, None,
            {"scope": "df openid", "code_challenge": True, "code_verifier": True}
    ),
    (
            "auto PKCE",
            "myclient", None, None,
            {"scope": "df openid", "code_challenge": ABSENT, "code_verifier": ABSENT}
    ),
    (
            "auto PKCE, default client with PKCE",
            "default-with-pkce", None, None,
            {"scope": "df openid", "code_challenge": True, "code_verifier": True}
    ),
    (
            "auto PKCE, default client no PKCE",
            "default-no-pkce", None, None,
            {"scope": "df openid", "code_challenge": ABSENT, "code_verifier": ABSENT}
    ),
    (
            "auto PKCE, default client with PKCE, and secret",
            "default-with-pkce", None, "$3cr3t",
            {"scope": "df openid", "client_secret": "$3cr3t", "code_challenge": ABSENT, "code_verifier": ABSENT}
    ),
])
def test_oidc_device_flow_auto_detect(
        requests_mock, caplog, mode, client_id, use_pkce, client_secret, expected_fields
):
    """Autodetection of device auth grant mode: with secret, PKCE or neither."""
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields=expected_fields,
        state={"device_code_callback_timeline": ["authorization_pending", "slow_down", "great success"]},
        scopes_supported=["openid", "df"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url, scopes=["df"], default_clients=[
        {"id": "default-with-pkce", "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce"]},
        {"id": "default-no-pkce", "grant_types": ["urn:ietf:params:oauth:grant-type:device_code"]},
    ])
    display = []
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        display=display.append,
        use_pkce=use_pkce
    )
    with mock.patch.object(openeo.rest.auth.oidc.time, "sleep") as sleep:
        with caplog.at_level(logging.INFO):
            tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token
    assert re.search(
        r"visit https://auth\.test/dc and enter the user code {c!r}".format(c=oidc_mock.state['user_code']),
        display[0]
    )
    assert display[1] == "Authorized successfully."
    assert sleep.mock_calls == [mock.call(2), mock.call(2), mock.call(7)]
    assert re.search(
        r"Authorization pending\..*Polling too fast, will slow down\..*Authorized successfully\.",
        caplog.text,
        flags=re.DOTALL
    )


def test_oidc_refresh_token_flow(requests_mock, caplog):
    client_id = "myclient"
    client_secret = "$3cr3t"
    refresh_token = "r3fr35h.d4.t0k3n.w1lly4"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={"scope": "openid", "client_secret": client_secret, "refresh_token": refresh_token},
        scopes_supported=["openid"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url)
    authenticator = OidcRefreshTokenAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        refresh_token=refresh_token
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_refresh_token_flow_no_secret(requests_mock, caplog):
    client_id = "myclient"
    refresh_token = "r3fr35h.d4.t0k3n.w1lly4"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={"scope": "openid", "refresh_token": refresh_token},
        scopes_supported=["openid"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url)
    authenticator = OidcRefreshTokenAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        refresh_token=refresh_token
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_refresh_token_invalid_token(requests_mock, caplog):
    client_id = "myclient"
    refresh_token = "wr0n9.t0k3n"
    oidc_discovery_url = "http://oidc.test/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        expected_fields={"scope": "openid", "refresh_token": "c0rr3ct.t0k3n"},
        scopes_supported=["openid"]
    )
    provider = OidcProviderInfo(discovery_url=oidc_discovery_url)
    authenticator = OidcRefreshTokenAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        refresh_token=refresh_token
    )
    with pytest.raises(OidcException, match="Failed to retrieve access token.*invalid refresh token"):
        tokens = authenticator.get_tokens()

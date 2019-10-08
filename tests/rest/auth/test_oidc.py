import base64
import json
import urllib.parse
import urllib.parse
from io import BytesIO
from queue import Queue

import requests
import requests_mock.request
import requests_mock.request

from openeo.rest.auth.oidc import QueuingRequestHandler, drain_queue, HttpServerThread, OidcAuthCodePkceAuthenticator, \
    OidcClientCredentialsAuthenticator, OidcResourceOwnerPasswordAuthenticator, OidcClientInfo


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


def test_http_server_thread():
    queue = Queue()
    server_thread = HttpServerThread(RequestHandlerClass=QueuingRequestHandler.with_queue(queue))
    server_thread.start()
    port, host, fqdn = server_thread.server_address_info()
    url = 'http://{f}:{p}/foo/bar'.format(f=fqdn, p=port)
    response = requests.get(url)
    response.raise_for_status()
    assert list(drain_queue(queue)) == ['/foo/bar']
    server_thread.shutdown()
    server_thread.join()


def test_http_server_thread_port():
    queue = Queue()
    server_thread = HttpServerThread(RequestHandlerClass=QueuingRequestHandler.with_queue(queue),
                                     server_address=('', 12345))
    server_thread.start()
    port, host, fqdn = server_thread.server_address_info()
    assert port == 12345
    url = 'http://{f}:{p}/foo/bar'.format(f=fqdn, p=port)
    response = requests.get(url)
    response.raise_for_status()
    assert list(drain_queue(queue)) == ['/foo/bar']
    server_thread.shutdown()
    server_thread.join()


class OidcMock:
    """
    Mock object to test OIDC flows
    """

    def __init__(
            self,
            requests_mock: requests_mock.Mocker,
            oidc_discovery_url: str,
            expected_grant_type: str,
            expected_client_id: str = "myclient",
            expected_fields: dict = None,
            provider_root_url: str = "https://auth.example.com"
    ):
        self.requests_mock = requests_mock
        self.oidc_discovery_url = oidc_discovery_url
        self.expected_grant_type = expected_grant_type
        self.expected_client_id = expected_client_id
        self.expected_fields = expected_fields
        self.expected_authorization_code = None
        self.provider_root_url = provider_root_url
        self.authorization_endpoint = provider_root_url + "/auth"
        self.token_endpoint = provider_root_url + "/token"
        self.state = {}

        self.requests_mock.get(oidc_discovery_url, text=json.dumps({
            # Rudimentary OpenID Connect discovery document
            "authorization_endpoint": self.authorization_endpoint,
            "token_endpoint": self.token_endpoint,
        }))
        self.requests_mock.post(
            self.token_endpoint,
            text={
                "authorization_code": self.token_callback_authorization_code,
                "client_credentials": self.token_callback_client_credentials,
                "password": self.token_callback_resource_owner_password_credentials,
            }[expected_grant_type]
        )

    def webbrowser_open(self, url: str):
        """Doing fake browser and Oauth Provider handling here"""
        assert url.startswith(self.authorization_endpoint)
        params = self._get_query_params(url=url)
        assert params["client_id"] == self.expected_client_id
        assert params["response_type"] == "code"
        for key in ["state", "nonce", "code_challenge", "redirect_uri"]:
            self.state[key] = params[key]
        redirect_uri = params["redirect_uri"]
        # Don't mock the request to the redirect URI (it is hosted by the temporary web server in separate thread)
        self.requests_mock.get(redirect_uri, real_http=True)
        self.expected_authorization_code = "6uthc0d3"
        requests.get(redirect_uri, params={"state": params["state"], "code": self.expected_authorization_code})

    def token_callback_authorization_code(self, request: requests_mock.request._RequestObjectProxy, context):
        """Fake code to token exchange by Oauth Provider"""
        params = self._get_query_params(query=request.text)
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "authorization_code"
        assert self.state["code_challenge"] == OidcAuthCodePkceAuthenticator.hash_code_verifier(params["code_verifier"])
        assert params["code"] == self.expected_authorization_code
        assert params["redirect_uri"] == self.state["redirect_uri"]
        self.state["access_token"] = self._jwt_encode({}, {"sub": "123", "name": "john", "nonce": self.state["nonce"]})
        return json.dumps({
            "access_token": self.state["access_token"],
            "id_token": self._jwt_encode({}, {"sub": "123", "name": "john", "nonce": self.state["nonce"]}),
            "refresh_token": self._jwt_encode({}, {}),
        })

    def token_callback_client_credentials(self, request: requests_mock.request._RequestObjectProxy, context):
        params = self._get_query_params(query=request.text)
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "client_credentials"
        assert params["client_secret"] == self.expected_fields["client_secret"]
        self.state["access_token"] = self._jwt_encode({}, {"sub": "123", "name": "john"})
        return json.dumps({
            "access_token": self.state["access_token"],
            "refresh_token": self._jwt_encode({}, {}),
        })

    def token_callback_resource_owner_password_credentials(self, request: requests_mock.request._RequestObjectProxy,
                                                           context):
        params = self._get_query_params(query=request.text)
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "password"
        assert params["username"] == self.expected_fields["username"]
        assert params["password"] == self.expected_fields["password"]
        self.state["access_token"] = self._jwt_encode({}, {"sub": "123", "name": "john"})
        return json.dumps({
            "access_token": self.state["access_token"],
            "id_token": self._jwt_encode({}, {"sub": "123", "name": "john"}),
            "refresh_token": self._jwt_encode({}, {}),
        })

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


def test_oidc_auth_code_pkce_flow(requests_mock):
    client_id = "myclient"
    oidc_discovery_url = "http://oidc.example.com/.well-known/openid-configuration"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        oidc_discovery_url=oidc_discovery_url
    )

    authenticator = OidcAuthCodePkceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, oidc_discovery_url=oidc_discovery_url),
        webbrowser_open=oidc_mock.webbrowser_open
    )
    # Do the Oauth/OpenID Connect flow
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_client_credentials_flow(requests_mock):
    client_id = "myclient"
    oidc_discovery_url = "http://oidc.example.com/.well-known/openid-configuration"
    client_secret = "$3cr3t"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret},
        oidc_discovery_url=oidc_discovery_url
    )

    authenticator = OidcClientCredentialsAuthenticator(
        client_info=OidcClientInfo(
            client_id=client_id,
            oidc_discovery_url=oidc_discovery_url,
            client_secret=client_secret
        )
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_resource_owner_password_credentials_flow(requests_mock):
    client_id = "myclient"
    oidc_discovery_url = "http://oidc.example.com/.well-known/openid-configuration"
    username, password = "john", "j0hn"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="password",
        expected_client_id=client_id,
        expected_fields={"username": username, "password": password},
        oidc_discovery_url=oidc_discovery_url
    )

    authenticator = OidcResourceOwnerPasswordAuthenticator(
        client_info=OidcClientInfo(
            client_id=client_id,
            oidc_discovery_url=oidc_discovery_url,
        ),
        username=username, password=password,
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token

# TODO test for OidcRefreshTokenAuthenticator

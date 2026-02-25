import logging
import re
import time
import urllib.parse
from io import BytesIO
from queue import Queue

import pytest
import requests
import requests_mock

from openeo.rest.auth.oidc import (
    DefaultOidcClientGrant,
    HttpServerThread,
    OidcAuthCodePkceAuthenticator,
    OidcClientCredentialsAuthenticator,
    OidcClientInfo,
    OidcDeviceAuthenticator,
    OidcException,
    OidcProviderInfo,
    OidcRefreshTokenAuthenticator,
    OidcResourceOwnerPasswordAuthenticator,
    QueuingRequestHandler,
    drain_queue,
)
from openeo.rest.auth.testing import ABSENT, OidcMock


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


def test_provider_info_issuer_custom_requests_session():
    # Instead of using the requests_mock fixture like we do in other tests:
    # explicitly create a requests session to play with, using an adapter from requests_mock
    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount("https://", adapter)
    adapter.register_uri(
        "GET",
        "https://authit.test/.well-known/openid-configuration",
        json={"scopes_supported": ["openid"]},
    )
    p = OidcProviderInfo(issuer="https://authit.test", requests_session=session)
    assert p.discovery_url == "https://authit.test/.well-known/openid-configuration"
    assert p.get_scopes_string() == "openid"
    assert len(adapter.request_history) == 1


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


def test_provider_info_issuer_broken_json(requests_mock):
    requests_mock.get("https://authit.test/.well-known/openid-configuration", text="<marquee>nope!</marquee>")
    with pytest.raises(
        OidcException,
        match="Failed to obtain OIDC discovery document from 'https://authit.test/.well-known/openid-configuration': JSONDecodeError",
    ):
        OidcProviderInfo(issuer="https://authit.test")


def test_oidc_client_info_uses_device_flow_pkce_support(requests_mock):
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_issuer=oidc_issuer,
    )
    provider = OidcProviderInfo(
        issuer=oidc_issuer,
        default_clients=[
            {"id": "c1", "grant_types": ["authorization_code+pkce"]},
            {
                "id": "c2",
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code"],
            },
            {
                "id": "c3",
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce"],
            },
            {
                "id": "c4",
                "grant_types": [
                    "refresh_token",
                    "urn:ietf:params:oauth:grant-type:device_code+pkce",
                ],
            },
        ],
    )

    for client_id, expected in [
        ("c1", False),
        ("c2", False),
        ("c3", True),
        ("c4", True),
        ("foo", False)
    ]:
        client_info = OidcClientInfo(client_id=client_id, provider=provider)
        assert client_info.guess_device_flow_pkce_support() is expected





@pytest.mark.slow
def test_oidc_auth_code_pkce_flow(requests_mock):
    requests_mock.get("http://oidc.test/.well-known/openid-configuration", json={"scopes_supported": ["openid"]})

    client_id = "myclient"
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid testpkce"},
        oidc_issuer=oidc_issuer,
        scopes_supported=["openid", "testpkce"]
    )
    provider = OidcProviderInfo(issuer=oidc_issuer, scopes=["openid", "testpkce"])
    authenticator = OidcAuthCodePkceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        webbrowser_open=oidc_mock.webbrowser_open
    )
    # Do the Oauth/OpenID Connect flow
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_client_credentials_flow(requests_mock):
    client_id = "myclient"
    oidc_issuer = "https://oidc.test"
    client_secret = "$3cr3t"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
        oidc_issuer=oidc_issuer,
    )

    provider = OidcProviderInfo(issuer=oidc_issuer)
    authenticator = OidcClientCredentialsAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret)
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_client_credentials_flow_connection_error(requests_mock):
    client_id = "myclient"
    oidc_issuer = "https://oidc.test"
    client_secret = "$3cr3t"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="client_credentials",
        expected_client_id=client_id,
        expected_fields={"client_secret": client_secret, "scope": "openid"},
        oidc_issuer=oidc_issuer,
    )
    requests_mock.post(oidc_mock.token_endpoint, exc=requests.exceptions.ConnectionError)

    provider = OidcProviderInfo(issuer=oidc_issuer)
    authenticator = OidcClientCredentialsAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret)
    )
    with pytest.raises(
        OidcException, match="Failed to retrieve access token at 'https://oidc.test/token': ConnectionError"
    ):
        _ = authenticator.get_tokens()


def test_oidc_resource_owner_password_credentials_flow(requests_mock):
    client_id = "myclient"
    client_secret = "$3cr3t"
    oidc_issuer = "https://oidc.test"
    username, password = "john", "j0hn"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="password",
        expected_client_id=client_id,
        expected_fields={
            "username": username, "password": password, "scope": "openid testpwd", "client_secret": client_secret
        },
        oidc_issuer=oidc_issuer,
        scopes_supported=["openid", "testpwd"],
    )
    provider = OidcProviderInfo(issuer=oidc_issuer, scopes=["testpwd"])

    authenticator = OidcResourceOwnerPasswordAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        username=username, password=password,
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token



class DeviceCodePollDisplay:
    def __init__(self):
        self.lines = []

    def __call__(self, line: str, end: str = "\n"):
        self.lines.append((time.time(), line, end))


@pytest.mark.parametrize("support_verification_uri_complete", [False, True])
def test_oidc_device_flow_with_client_secret(requests_mock, caplog, support_verification_uri_complete, simple_time):
    client_id = "myclient"
    client_secret = "$3cr3t"
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"scope": "df openid", "client_secret": client_secret},
        state={"device_code_callback_timeline": ["authorization_pending", "slow_down", "great success"]},
        scopes_supported=["openid", "df"],
        support_verification_uri_complete=support_verification_uri_complete,
    )
    provider = OidcProviderInfo(issuer=oidc_issuer, scopes=["df"])

    display = DeviceCodePollDisplay()
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        display=display,
        max_poll_time=20,
    )
    with caplog.at_level(logging.INFO):
        tokens = authenticator.get_tokens()

    assert oidc_mock.state["access_token"] == tokens.access_token
    user_code = oidc_mock.state["user_code"]

    assert 5 < len(display.lines) < 20

    if support_verification_uri_complete:
        expected_instruction = f"Visit https://oidc.test/dc?user_code={user_code} to authenticate."
    else:
        expected_instruction = f"Visit https://oidc.test/dc and enter user code {user_code!r} to authenticate."
    assert display.lines[0] == (1000, expected_instruction, "\n")

    for expected_line in [
        (1000, "[#####################################] Authorization pending                   ", "\r"),
        (1002, "[#################################----] Polling                                 ", "\r"),
        (1002, "[#################################----] Authorization pending                   ", "\r"),
        (1004, "[##############################-------] Polling                                 ", "\r"),
        (1004, "[##############################-------] Slowing down                            ", "\r"),
        (1010, "[##################-------------------] Slowing down                            ", "\r"),
    ]:
        assert expected_line in display.lines

    assert display.lines[-3:] == [
        (1011, "[#################--------------------] Polling                                 ", "\r"),
        (1011, "Authorized successfully                                                         ", "\r"),
        (1011, "", "\n"),
    ]

    assert re.search(r"authorization_pending.*slow_down.*Authorized successfully", caplog.text, flags=re.DOTALL)


@pytest.mark.parametrize("support_verification_uri_complete", [False, True])
def test_oidc_device_flow_with_pkce(requests_mock, caplog, support_verification_uri_complete, simple_time):
    client_id = "myclient"
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"scope": "df openid", "code_challenge": True, "code_verifier": True},
        state={
            "device_code_callback_timeline": [
                "authorization_pending",
                "authorization_pending",
                "authorization_pending",
                "authorization_pending",
                "authorization_pending",
                "authorization_pending",
                "authorization_pending",
                "great success",
            ]
        },
        scopes_supported=["openid", "df"],
        support_verification_uri_complete=support_verification_uri_complete,
    )
    provider = OidcProviderInfo(issuer=oidc_issuer, scopes=["df"])
    display = DeviceCodePollDisplay()
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        display=display,
        use_pkce=True,
        max_poll_time=60,
    )
    with caplog.at_level(logging.INFO):
        tokens = authenticator.get_tokens()

    assert oidc_mock.state["access_token"] == tokens.access_token
    user_code = oidc_mock.state["user_code"]

    assert 5 < len(display.lines) < 40

    if support_verification_uri_complete:
        expected_instruction = f"Visit https://oidc.test/dc?user_code={user_code} to authenticate."
    else:
        expected_instruction = f"Visit https://oidc.test/dc and enter user code {user_code!r} to authenticate."
    assert display.lines[0] == (1000, expected_instruction, "\n")

    for expected_line in [
        (1000, "[#####################################] Authorization pending                   ", "\r"),
        (1002, "[####################################-] Polling                                 ", "\r"),
        (1002, "[####################################-] Authorization pending                   ", "\r"),
        (1006, "[#################################----] Polling                                 ", "\r"),
        (1006, "[#################################----] Authorization pending                   ", "\r"),
        (1013, "[#############################--------] Authorization pending                   ", "\r"),
        (1014, "[############################---------] Polling                                 ", "\r"),
        (1014, "[############################---------] Authorization pending                   ", "\r"),
    ]:
        assert expected_line in display.lines

    assert display.lines[-3:] == [
        (1016, "[###########################----------] Polling                                 ", "\r"),
        (1016, "Authorized successfully                                                         ", "\r"),
        (1016, "", "\n"),
    ]

    assert re.search(
        r"authorization_pending.*authorization_pending.*Authorized successfully",
        caplog.text,
        flags=re.DOTALL,
    )


@pytest.mark.parametrize("support_verification_uri_complete", [False, True])
def test_oidc_device_flow_without_pkce_nor_secret(
    requests_mock, caplog, support_verification_uri_complete, simple_time
):
    client_id = "myclient"
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"scope": "df openid", "code_challenge": ABSENT, "code_verifier": ABSENT},
        state={
            "device_code_callback_timeline": [
                "authorization_pending",
                "slow_down",
                "authorization_pending",
                "authorization_pending",
                "authorization_pending",
                "authorization_pending",
                "great success",
            ]
        },
        scopes_supported=["openid", "df"],
        support_verification_uri_complete=support_verification_uri_complete,
    )
    provider = OidcProviderInfo(issuer=oidc_issuer, scopes=["df"])
    display = DeviceCodePollDisplay()
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        display=display,
        max_poll_time=150,
    )

    with caplog.at_level(logging.INFO):
        tokens = authenticator.get_tokens()

    assert oidc_mock.state["access_token"] == tokens.access_token
    user_code = oidc_mock.state["user_code"]

    assert 5 < len(display.lines) < 50

    if support_verification_uri_complete:
        expected_instruction = f"Visit https://oidc.test/dc?user_code={user_code} to authenticate."
    else:
        expected_instruction = f"Visit https://oidc.test/dc and enter user code {user_code!r} to authenticate."
    assert display.lines[0] == (1000, expected_instruction, "\n")

    for expected_line in [
        (1000.0, "[#####################################] Authorization pending                   ", "\r"),
        (1001.5, "[#####################################] Authorization pending                   ", "\r"),
        (1003.0, "[####################################-] Polling                                 ", "\r"),
        (1003.0, "[####################################-] Authorization pending                   ", "\r"),
        (1006.0, "[####################################-] Polling                                 ", "\r"),
        (1006.0, "[####################################-] Slowing down                            ", "\r"),
        (1012.0, "[##################################---] Slowing down                            ", "\r"),
        (1013.5, "[##################################---] Polling                                 ", "\r"),
        (1013.5, "[##################################---] Authorization pending                   ", "\r"),
        (1028.5, "[##############################-------] Polling                                 ", "\r"),
        (1028.5, "[##############################-------] Authorization pending                   ", "\r"),
        (1042.0, "[###########################----------] Authorization pending                   ", "\r"),
    ]:
        assert expected_line in display.lines

    assert display.lines[-3:] == [
        (1043.5, "[##########################-----------] Polling                                 ", "\r"),
        (1043.5, "Authorized successfully                                                         ", "\r"),
        (1043.5, "", "\n"),
    ]

    assert re.search(r"authorization_pending.*slow_down.*Authorized successfully", caplog.text, flags=re.DOTALL)


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
@pytest.mark.parametrize("support_verification_uri_complete", [False, True])
def test_oidc_device_flow_auto_detect(
    requests_mock,
    caplog,
    mode,
    client_id,
    use_pkce,
    client_secret,
    expected_fields,
    support_verification_uri_complete,
    simple_time,
):
    """Autodetection of device auth grant mode: with secret, PKCE or neither."""
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields=expected_fields,
        state={
            "device_code_callback_timeline": [
                "authorization_pending",
                "slow_down",
                "authorization_pending",
                "authorization_pending",
                "great success",
            ]
        },
        scopes_supported=["openid", "df"],
        support_verification_uri_complete=support_verification_uri_complete,
    )
    provider = OidcProviderInfo(
        issuer=oidc_issuer,
        scopes=["df"],
        default_clients=[
            {
                "id": "default-with-pkce",
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce"],
            },
            {
                "id": "default-no-pkce",
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code"],
            },
        ],
    )
    display = DeviceCodePollDisplay()
    authenticator = OidcDeviceAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        display=display,
        use_pkce=use_pkce
    )

    with caplog.at_level(logging.INFO):
        tokens = authenticator.get_tokens()

    assert oidc_mock.state["access_token"] == tokens.access_token
    user_code = oidc_mock.state["user_code"]

    assert 10 < len(display.lines) < 50

    if support_verification_uri_complete:
        expected_instruction = f"Visit https://oidc.test/dc?user_code={user_code} to authenticate."
    else:
        expected_instruction = f"Visit https://oidc.test/dc and enter user code {user_code!r} to authenticate."
    assert display.lines[0] == (1000, expected_instruction, "\n")

    for expected_line in [
        (1000, "[#####################################] Authorization pending                   ", "\r"),
        (1015, "[###################################--] Polling                                 ", "\r"),
        (1015, "[###################################--] Authorization pending                   ", "\r"),
        (1024, "[##################################---] Authorization pending                   ", "\r"),
        (1027, "[##################################---] Authorization pending                   ", "\r"),
    ]:
        assert expected_line in display.lines

    assert display.lines[-3:] == [
        (1033, "[#################################----] Polling                                 ", "\r"),
        (1033, "Authorized successfully                                                         ", "\r"),
        (1033, "", "\n"),
    ]

    assert re.search(r"authorization_pending.*slow_down.*Authorized successfully", caplog.text, flags=re.DOTALL)


def test_oidc_refresh_token_flow(requests_mock, caplog):
    client_id = "myclient"
    client_secret = "$3cr3t"
    refresh_token = "r3fr35h.d4.t0k3n.w1lly4"
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"scope": "openid", "client_secret": client_secret, "refresh_token": refresh_token},
        scopes_supported=["openid"]
    )
    provider = OidcProviderInfo(issuer=oidc_issuer)
    authenticator = OidcRefreshTokenAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret),
        refresh_token=refresh_token
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_refresh_token_flow_no_secret(requests_mock, caplog):
    client_id = "myclient"
    refresh_token = "r3fr35h.d4.t0k3n.w1lly4"
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"scope": "openid", "refresh_token": refresh_token},
        scopes_supported=["openid"]
    )
    provider = OidcProviderInfo(issuer=oidc_issuer)
    authenticator = OidcRefreshTokenAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        refresh_token=refresh_token
    )
    tokens = authenticator.get_tokens()
    assert oidc_mock.state["access_token"] == tokens.access_token


def test_oidc_refresh_token_invalid_token(requests_mock, caplog):
    client_id = "myclient"
    refresh_token = "wr0n9.t0k3n"
    oidc_issuer = "https://oidc.test"
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="refresh_token",
        expected_client_id=client_id,
        oidc_issuer=oidc_issuer,
        expected_fields={"scope": "openid", "refresh_token": "c0rr3ct.t0k3n"},
        scopes_supported=["openid"]
    )
    provider = OidcProviderInfo(issuer=oidc_issuer)
    authenticator = OidcRefreshTokenAuthenticator(
        client_info=OidcClientInfo(client_id=client_id, provider=provider),
        refresh_token=refresh_token
    )
    with pytest.raises(OidcException, match="Failed to retrieve access token.*invalid refresh token"):
        tokens = authenticator.get_tokens()


def test_oidc_refresh_token_flow_custom_requests_session():
    client_id = "myclient"
    client_secret = "53cr3t"
    refresh_token = "r3fr35h.d4.t0k3n.w1lly4"
    oidc_issuer = "https://oidc.test"

    # Instead of using the OidcMock helper and related requests_mock fixture like we do in other tests:
    # explicitly create a requests session to play with, using an adapter from requests_mock
    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount("https://", adapter)
    adapter.register_uri(
        "GET",
        f"{oidc_issuer}/.well-known/openid-configuration",
        json={
            "issuer": oidc_issuer,
            "scopes_supported": ["openid"],
            "token_endpoint": f"{oidc_issuer}/token",
        },
    )

    def post_token(request, context):
        # Very simple handler here (compared to OidcMock implementation)
        assert f"refresh_token={refresh_token}" in request.text
        return {"access_token": "6cce5-t0k3n"}

    adapter.register_uri("POST", f"{oidc_issuer}/token", json=post_token)

    # Pass custom requests session
    provider = OidcProviderInfo(
        issuer=oidc_issuer,
        requests_session=session,
    )
    authenticator = OidcRefreshTokenAuthenticator(
        client_info=OidcClientInfo(
            client_id=client_id, provider=provider, client_secret=client_secret
        ),
        refresh_token=refresh_token,
        requests_session=session,
    )
    tokens = authenticator.get_tokens()

    assert tokens.access_token == "6cce5-t0k3n"
    assert len(adapter.request_history) == 2


class TestOidcProviderInfoAuthorizationParameters:
    """Tests for the authorization_parameters flag introduced in openEO API >= 1.3.0"""

    def test_from_dict_with_authorization_parameters(self, requests_mock):
        requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"scopes_supported": ["openid"]})
        data = {
            "id": "google",
            "title": "Google",
            "issuer": "https://authit.test",
            "scopes": ["openid"],
            "authorization_parameters": {"access_type": "offline", "prompt": "consent"},
        }
        info = OidcProviderInfo.from_dict(data)
        assert info.authorization_parameters == {"access_type": "offline", "prompt": "consent"}

    def test_from_dict_without_authorization_parameters(self, requests_mock):
        requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"scopes_supported": ["openid"]})
        data = {
            "id": "egi",
            "title": "EGI",
            "issuer": "https://authit.test",
        }
        info = OidcProviderInfo.from_dict(data)
        assert info.authorization_parameters == {}

    def test_device_code_request_includes_authorization_parameters(self, requests_mock):
        """Checks whether the authorization_parameters ends up in the device code POST body."""
        oidc_issuer = "https://authit.test"
        requests_mock.get(
            f"{oidc_issuer}/.well-known/openid-configuration",
            json={
                "scopes_supported": ["openid"],
                "device_authorization_endpoint": f"{oidc_issuer}/device_code",
                "token_endpoint": f"{oidc_issuer}/token",
            },
        )
        device_code_mock = requests_mock.post(
            f"{oidc_issuer}/device_code",
            json={
                "device_code": "d3v1c3",
                "user_code": "US3R",
                "verification_uri": f"{oidc_issuer}/dc",
                "interval": 5,
            },
        )
        provider = OidcProviderInfo(
            issuer=oidc_issuer,
            authorization_parameters={"access_type": "offline", "prompt": "consent"},
        )
        authenticator = OidcDeviceAuthenticator(
            client_info=OidcClientInfo(client_id="myclient", provider=provider, client_secret="s3cr3t"),
        )
        authenticator._get_verification_info()

        assert device_code_mock.call_count == 1
        post_body = urllib.parse.parse_qs(device_code_mock.last_request.text)
        assert post_body["client_id"] == ["myclient"]
        assert post_body["access_type"] == ["offline"]
        assert post_body["prompt"] == ["consent"]

    def test_device_code_request_without_authorization_parameters(self, requests_mock):
        """Verify no extra params when authorization_parameters is empty."""
        oidc_issuer = "https://authit.test"
        requests_mock.get(
            f"{oidc_issuer}/.well-known/openid-configuration",
            json={
                "scopes_supported": ["openid"],
                "device_authorization_endpoint": f"{oidc_issuer}/device_code",
                "token_endpoint": f"{oidc_issuer}/token",
            },
        )
        device_code_mock = requests_mock.post(
            f"{oidc_issuer}/device_code",
            json={
                "device_code": "d3v1c3",
                "user_code": "US3R",
                "verification_uri": f"{oidc_issuer}/dc",
                "interval": 5,
            },
        )
        provider = OidcProviderInfo(issuer=oidc_issuer)
        authenticator = OidcDeviceAuthenticator(
            client_info=OidcClientInfo(client_id="myclient", provider=provider, client_secret="s3cr3t"),
        )
        authenticator._get_verification_info()

        post_body = urllib.parse.parse_qs(device_code_mock.last_request.text)
        assert "access_type" not in post_body
        assert "prompt" not in post_body
        assert set(post_body.keys()) == {"client_id", "scope"}

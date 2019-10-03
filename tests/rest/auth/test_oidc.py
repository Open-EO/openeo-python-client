import http.server
from io import BytesIO
from queue import Queue
from typing import Type

import requests

from openeo.rest.auth.oidc import QueuingRequestHandler, drain_queue, HttpServerThread, OidcAuthCodePkceAuthenticator


def handle_request(handler_class: Type[http.server.BaseHTTPRequestHandler], path: str):
    """Fake (serverless) request handling"""

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


def test_oidc_flow(oidc_test_setup):
    # see test/rest/conftest.py for `oidc_test_setup` fixture
    client_id = "myclient"
    oidc_discovery_url = "http://oidc.example.com/.well-known/openid-configuration"
    state, webbrowser_open = oidc_test_setup(client_id=client_id, oidc_discovery_url=oidc_discovery_url)

    authenticator = OidcAuthCodePkceAuthenticator(
        client_id=client_id,
        oidc_discovery_url=oidc_discovery_url,
        webbrowser_open=webbrowser_open
    )
    # Do the Oauth/OpenID Connect flow
    tokens = authenticator.get_tokens()
    assert state["access_token"] == tokens.access_token

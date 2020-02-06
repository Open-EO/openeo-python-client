"""
OpenID Connect related functionality and helpers.

"""

import base64
import functools
import hashlib
import http.server
import json
import logging
import random
import string
import threading
import time
import urllib.parse
import warnings
import webbrowser
from collections import namedtuple
from queue import Queue, Empty
from typing import Tuple, Callable

import requests
from openeo.rest import OpenEoClientException

log = logging.getLogger(__name__)


class QueuingRequestHandler(http.server.BaseHTTPRequestHandler):
    """
    Base class for simple HTTP request handlers to be used in threaded context.
    The handler puts the requested paths in a thread-safe queue
    """

    def __init__(self, *args, **kwargs):
        self._queue = kwargs.pop('queue', None) or Queue()
        super().__init__(*args, **kwargs)

    def do_GET(self):
        log.debug('{c} GET {p}'.format(c=self.__class__.__name__, p=self.path))
        self.queue(self.path)

    def queue(self, path: str):
        self._queue.put(path)
        self.send_response(200)
        self.end_headers()

    @classmethod
    def with_queue(cls, queue: Queue):
        """Create a factory for this object pre-bound with given queue object"""
        return functools.partial(cls, queue=queue)

    def log_message(self, format, *args):
        # Override the default implementation, which is a hardcoded `sys.stderr.write`
        log.debug(format % args)


class OAuthRedirectRequestHandler(QueuingRequestHandler):
    """Request handler for OAuth redirects"""
    PATH = '/callback'

    def queue(self, path: str):
        if path.startswith(self.PATH + '?'):
            super().queue(path)
            # TODO: auto-close browser tab/window?
            # TODO: have a nicer page with title and bit of metadata?
            self.wfile.write(b'You can close this browser tab/window now.')


class HttpServerThread(threading.Thread):
    """
    Thread that runs a HTTP server (`http.server.HTTPServer`)
    """

    def __init__(self, RequestHandlerClass, server_address: Tuple[str, int] = None):
        # Make it a daemon to minimize potential shutdown issues due to `serve_forever`
        super().__init__(daemon=True)
        self._RequestHandlerClass = RequestHandlerClass
        # Server address ('', 0): listen on all ips and let OS pick a free port
        self._server_address = server_address or ('', 0)
        self._server = None

    def start(self):
        self._server = http.server.HTTPServer(self._server_address, self._RequestHandlerClass)
        self._log_status("start thread")
        super().start()

    def run(self):
        self._log_status("start serving")
        self._server.serve_forever()
        self._log_status("stop serving")

    def shutdown(self):
        self._log_status("shut down thread")
        self._server.shutdown()

    def server_address_info(self) -> Tuple[int, str, str]:
        """
        Get server address info: (port, host_address, fully_qualified_domain_name)
        """
        if self._server is None:
            raise RuntimeError("Server is not set up yet")
        return self._server.server_port, self._server.server_address[0], self._server.server_name

    def _log_status(self, message):
        port, host, fqdn = self.server_address_info()
        log.info("{c}: {m} (at {h}:{p}, {f})".format(c=self.__class__.__name__, m=message, h=host, p=port, f=fqdn))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        self.join()
        self._log_status("thread joined")


def drain_queue(queue: Queue, initial_timeout: float = 10, item_minimum: int = 1, tail_timeout=5):
    """
    Drain the given queue, requiring at least a given number of items (within an initial timeout).

    :param queue: queue to drain
    :param initial_timeout: time in seconds within which a minimum number of items should be fetched
    :param item_minimum: minimum number of items to fetch
    :param tail_timeout: additional timeout to abort when queue doesn't get empty
    :return: generator of items from the queue
    """
    start = time.time()
    count = 0
    while True:
        try:
            yield queue.get(timeout=initial_timeout / 10)
            count += 1
        except Empty:
            pass
        now = time.time()

        if now > start + initial_timeout and count < item_minimum:
            raise TimeoutError("Items after initial {t} timeout: {c} (<{m})".format(
                c=count, m=item_minimum, t=initial_timeout))
        if queue.empty() and count >= item_minimum:
            break
        if now > start + initial_timeout + tail_timeout:
            warnings.warn("Queue still not empty after overall timeout: aborting.")
            break


def random_string(length=32, characters: str = None):
    """
    Build a random string from given characters (alphanumeric by default)

    TODO: move this to a utils module?
    """
    characters = characters or (string.ascii_letters + string.digits)
    return "".join(random.choice(characters) for _ in range(length))


class OAuthException(OpenEoClientException):
    pass


class OidcAuthenticator:
    pass


class OidcAuthCodePkceAuthenticator(OidcAuthenticator):
    """
    Implementation of OpenID Connect authentication using OAuth Authorization Code Flow with PKCE.

    This flow is to be used for interactive use cases (e.g. user is working in a Jupyter/IPython notebook).

    It goes roughly like this:
    - A short living HTTP server is started in a side-thread to serve the redirect URI
        that is required in this flow.
    - A browser window/tab is opened showing the (third party) Identity Provider authorization endpoint
    - (if not already:) User authenticates with the Identity Provider (e.g. with username and password)
    - Identity Provider forwards to the redirect URI (which is served locally by the side-thread),
        sending an authorization code (among others) along
    - The request handler in the side thread captures the redirect and passes it to the main thread (through a queue)
    - The main extracts the necessary information from the redirect request (like the authorization code)
        and shuts down the side thread
    - The authorization code is exchanged for an access code and id token
    - The access code can be used as bearer token for subsequent API calls
    """

    AuthCodeResult = namedtuple("AuthCodeResult", ["auth_code", "nonce", "code_verifier", "redirect_uri"])
    AccessTokenResult = namedtuple("AccessTokenResult", ["access_token", "id_token", "refresh_token"])

    def __init__(self, client_id: str, oidc_discovery_url: str, webbrowser_open: Callable = None, timeout=120,
                 server_address: Tuple[str, int] = None):
        self._client_id = client_id
        self._provider_info = requests.get(oidc_discovery_url).json()
        self._webbrowser_open = webbrowser_open or webbrowser.open
        self._authentication_timeout = timeout
        self._server_address = server_address

    @staticmethod
    def hash_code_verifier(code: str) -> str:
        """Hash code verifier to code challenge"""
        return base64.urlsafe_b64encode(
            hashlib.sha256(code.encode('ascii')).digest()
        ).decode('ascii').replace('=', '')

    @staticmethod
    def get_pkce_codes() -> Tuple[str, str]:
        """Build random PKCE code verifier and challenge"""
        code_verifier = random_string(64)
        code_challenge = OidcAuthCodePkceAuthenticator.hash_code_verifier(code_verifier)
        return code_verifier, code_challenge

    def _get_auth_code(self) -> AuthCodeResult:
        """
        Do OAuth authentication request and catch redirect to extract authentication code
        :return:
        """
        state = random_string(32)
        nonce = random_string(21)
        code_verifier, code_challenge = self.get_pkce_codes()
        # TODO: maybe just the openid scope is enough?
        supported_scopes = set(self._provider_info.get('scopes_supported', []))
        scopes = supported_scopes.intersection({"openid", "email", "profile"})

        # Set up HTTP server (in separate thread) to catch OAuth redirect URL
        callback_queue = Queue()
        RequestHandlerClass = OAuthRedirectRequestHandler.with_queue(callback_queue)
        http_server_thread = HttpServerThread(
            RequestHandlerClass=RequestHandlerClass,
            server_address=self._server_address
        )
        with http_server_thread:
            port, host, fqdn = http_server_thread.server_address_info()
            # TODO: use fully qualified domain name instead of "localhost"?
            #       Otherwise things won't work when the client is for example
            #       running in a remotely hosted Jupyter setup.
            #       Maybe even FQDN will not resolve properly in the user's browser
            #       and we need additional means to get a working hostname?
            redirect_uri = 'http://localhost:{p}'.format(f=fqdn, p=port) + OAuthRedirectRequestHandler.PATH
            log.info("Using OAuth redirect URI {u}".format(u=redirect_uri))

            # Build authentication URL
            auth_url = "{endpoint}?{params}".format(
                endpoint=self._provider_info['authorization_endpoint'],
                params=urllib.parse.urlencode({
                    "response_type": "code",
                    "client_id": self._client_id,
                    "scope": " ".join(scopes),
                    "redirect_uri": redirect_uri,
                    "state": state,
                    "nonce": nonce,
                    "code_challenge": code_challenge,
                    "code_challenge_method": "S256",
                })
            )
            log.info("Sending user to auth URL {u}".format(u=auth_url))
            # Open browser window/tab with authentication URL
            self._webbrowser_open(auth_url)

            # TODO: show some feedback here that we are waiting browser based interaction here?

            try:
                # Collect data from redirect uri
                # TODO: When authentication fails (e.g. identity provider is down), this might hang the client (e.g. jupyter notebook). Is there a way to abort this?
                callbacks = list(drain_queue(callback_queue, initial_timeout=self._authentication_timeout))
            except TimeoutError:
                raise OAuthException("Failed to collect OAuth authorization code from redirect (timeout={t}s)".format(
                    t=self._authentication_timeout)
                )

        if len(callbacks) != 1:
            raise OAuthException("Expected 1 OAuth redirect request, but got: {c}".format(c=len(callbacks)))

        # Parse OAuth redirect URL
        redirect_request = callbacks[0]
        log.debug("Parsing redirect request {r}".format(r=redirect_request))
        redirect_params = urllib.parse.parse_qs(urllib.parse.urlparse(redirect_request).query)
        log.debug('Parsed redirect request: {p}'.format(p=redirect_params))
        if 'state' not in redirect_params or redirect_params['state'] != [state]:
            raise OAuthException("Invalid state")
        if 'code' not in redirect_params:
            raise OAuthException("No auth code in redirect")
        auth_code = redirect_params["code"][0]

        return self.AuthCodeResult(auth_code=auth_code, nonce=nonce, code_verifier=code_verifier,
                                   redirect_uri=redirect_uri)

    def get_tokens(self) -> AccessTokenResult:
        """
        Do OpenID authentication flow with PKCE:
        get auth code and exchange for access and id token
        """
        # Get auth code from authentication provider
        auth_code_result = self._get_auth_code()

        # Resolve auth code to access token and id token
        token_endpoint = self._provider_info['token_endpoint']
        log.info("Exchanging auth code for access token at {u}".format(u=token_endpoint))
        token_response = requests.post(
            url=token_endpoint,
            data={
                "grant_type": "authorization_code",
                "client_id": self._client_id,
                "redirect_uri": auth_code_result.redirect_uri,
                "code": auth_code_result.auth_code,
                "code_verifier": auth_code_result.code_verifier,
            },
        )
        token_response.raise_for_status()

        result = token_response.json()
        log.debug("Token response with keys {k}".format(k=result.keys()))

        def extract_token(key):
            try:
                token = result[key]
            except KeyError:
                raise OAuthException("No {k} in response".format(k=key))
            # TODO: verify the JWT properly?
            _, payload = jwt_decode(token)
            if payload['nonce'] != auth_code_result.nonce:
                raise OAuthException("Invalid nonce in {k}".format(k=key))
            return token

        access_token = extract_token("access_token")
        id_token = extract_token("id_token")
        refresh_token = extract_token("refresh_token")
        return self.AccessTokenResult(
            access_token=access_token,
            id_token=id_token,
            refresh_token=refresh_token
        )


def jwt_decode(token: str) -> Tuple[dict, dict]:
    """
    Poor man's JWT decoding
    TODO: use a real library that also handles verification properly?
    """

    def _decode(data: str) -> dict:
        decoded = base64.b64decode(data + '=' * (4 - len(data) % 4)).decode('ascii')
        return json.loads(decoded)

    header, payload, signature = token.split('.')
    return _decode(header), _decode(payload)

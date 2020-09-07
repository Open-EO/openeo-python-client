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
from typing import Tuple, Callable, Union, List

import requests

import openeo
from openeo.rest import OpenEoClientException
from openeo.util import dict_no_none

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
        status, body, headers = self.queue(self.path)
        self.send_response(status)
        self.send_header('Content-Length', str(len(body)))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def queue(self, path: str):
        self._queue.put(path)
        return 200, "queued", {}

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

    TEMPLATE = """
        <!doctype html><html>
        <head><title>openEO OIDC auth</title></head>
        <body>
            {content}
            <hr><p><small>openEO Python client {version}</small></p>
        </body>
        </html>
    """

    def queue(self, path: str):
        if path.startswith(self.PATH + '?'):
            super().queue(path)
            # TODO: auto-close browser tab/window?
            # TODO: make it a nicer page and bit more of metadata?
            status = 200
            content = "<h1>OIDC Redirect URL request received.</h1><p>You can close this browser tab now.</p>"
        else:
            status = 404
            content = "<p>Not found.</p>"
        body = self.TEMPLATE.format(content=content, version=openeo.client_version())
        return status, body, {"Content-Type": "text/html; charset=UTF-8"}


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


def create_timer() -> Callable[[], float]:
    """Create a timer function that returns elapsed time since creation of the timer function"""
    start = time.time()

    def elapsed():
        return time.time() - start

    return elapsed


def drain_queue(queue: Queue, initial_timeout: float = 10, item_minimum: int = 1, tail_timeout=5,
                on_empty=lambda **kwargs: None):
    """
    Drain the given queue, requiring at least a given number of items (within an initial timeout).

    :param queue: queue to drain
    :param initial_timeout: time in seconds within which a minimum number of items should be fetched
    :param item_minimum: minimum number of items to fetch
    :param tail_timeout: additional timeout to abort when queue doesn't get empty
    :param on_empty: callable to call when/while queue is empty
    :return: generator of items from the queue
    """
    elapsed = create_timer()

    count = 0
    while True:
        try:
            yield queue.get(timeout=initial_timeout / 10)
            count += 1
        except Empty:
            on_empty(elapsed=elapsed(), count=count)

        if elapsed() > initial_timeout and count < item_minimum:
            raise TimeoutError("Items after initial {t} timeout: {c} (<{m})".format(
                c=count, m=item_minimum, t=initial_timeout))
        if queue.empty() and count >= item_minimum:
            break
        if elapsed() > initial_timeout + tail_timeout:
            warnings.warn("Queue still not empty after overall timeout: aborting.")
            break


def random_string(length=32, characters: str = None):
    """
    Build a random string from given characters (alphanumeric by default)

    TODO: move this to a utils module?
    """
    characters = characters or (string.ascii_letters + string.digits)
    return "".join(random.choice(characters) for _ in range(length))


class OidcException(OpenEoClientException):
    pass


# Container for result of access_token request.
AccessTokenResult = namedtuple(
    typename="AccessTokenResult",
    field_names=["access_token", "id_token", "refresh_token"]
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


class OidcProviderInfo:
    """OpenID Connect Provider information, as provided by an openEO back-end (endpoint `/credentials/oidc`)"""

    def __init__(self, issuer: str = None, discovery_url: str = None, scopes: List[str] = None):
        if issuer is None and discovery_url is None:
            raise ValueError("At least `issuer` or `discovery_url` should be specified")
        self.discovery_url = discovery_url or (issuer.rstrip("/") + "/.well-known/openid-configuration")
        discovery_resp = requests.get(self.discovery_url, timeout=20)
        discovery_resp.raise_for_status()
        self.config = discovery_resp.json()
        self.issuer = issuer or self.config["issuer"]
        # Scopes to request
        self._scopes = {
            "openid",
            # `offline_access` is required to get refresh tokens with Microsoft Identity platform
            "offline_access",
        }.union(scopes or []).intersection(self.config.get("scopes_supported", ["openid"]))

    def get_scopes_string(self):
        return " ".join(sorted(self._scopes))


class OidcClientInfo:
    """
    Simple container holding basic info of an OIDC client
    """

    def __init__(self, client_id: str, provider: OidcProviderInfo, client_secret: str = None):
        self.client_id = client_id
        self.provider = provider
        self.client_secret = client_secret
        # TODO: also info client type (desktop app, web app, SPA, ...)?

    # TODO: load from config file


class OidcAuthenticator:
    """
    Base class for OpenID Connect authentication flows.
    """
    grant_type = NotImplemented

    def __init__(self, client_info: OidcClientInfo):
        self._client_info = client_info
        self._provider_config = client_info.provider.config
        # TODO: check provider config (e.g. if grant type is supported)

    @property
    def client_id(self) -> str:
        return self._client_info.client_id

    @property
    def client_secret(self) -> str:
        return self._client_info.client_secret

    @property
    def provider_info(self) -> OidcProviderInfo:
        return self._client_info.provider

    def get_tokens(self) -> AccessTokenResult:
        """Get access_token and possibly id_token+refresh_token."""
        result = self._do_token_post_request(post_data=self._get_token_endpoint_post_data())
        return self._get_access_token_result(result)

    def _get_token_endpoint_post_data(self) -> dict:
        """Build POST data dict to send to token endpoint"""
        return {
            "grant_type": self.grant_type,
            "client_id": self.client_id,
        }

    def _do_token_post_request(self, post_data: dict) -> dict:
        """Do POST to token endpoint to get access token"""
        token_endpoint = self._provider_config['token_endpoint']
        log.info("Doing {g!r} token request {u!r} with post data fields {p!r} (client_id {c!r})".format(
            g=self.grant_type, c=self.client_id, u=token_endpoint, p=list(post_data.keys()))
        )
        resp = requests.post(url=token_endpoint, data=post_data)
        if resp.status_code != 200:
            # TODO: are other status_code values valid too?
            raise OidcException("Failed to retrieve access token at {u!r}: {s} {r!r} {t!r}".format(
                s=resp.status_code, r=resp.reason, u=resp.url, t=resp.text
            ))

        result = resp.json()
        log.debug("Token response with keys {k}".format(k=result.keys()))
        return result

    def _get_access_token_result(self, data: dict, expected_nonce: str = None) -> AccessTokenResult:
        """Parse JSON result from token request"""
        return AccessTokenResult(
            access_token=self._extract_token(data, "access_token"),
            id_token=self._extract_token(data, "id_token", expected_nonce=expected_nonce, allow_absent=True),
            refresh_token=self._extract_token(data, "refresh_token", allow_absent=True)
        )

    @staticmethod
    def _extract_token(data: dict, key: str, expected_nonce: str = None, allow_absent=False) -> Union[str, None]:
        """
        Extract token of given type ("access_token", "id_token", "refresh_token") from a token JSON response
        """
        try:
            token = data[key]
        except KeyError:
            if allow_absent:
                return
            raise OidcException("No {k!r} in response".format(k=key))
        if expected_nonce:
            # TODO: verify the JWT properly?
            _, payload = jwt_decode(token)
            if payload['nonce'] != expected_nonce:
                raise OidcException("Invalid nonce in {k}".format(k=key))
        return token


AuthCodeResult = namedtuple("AuthCodeResult", ["auth_code", "nonce", "code_verifier", "redirect_uri"])


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

    grant_type = "authorization_code"

    TIMEOUT_DEFAULT = 60

    def __init__(
            self,
            client_info: OidcClientInfo,
            webbrowser_open: Callable = None, timeout: int = None, server_address: Tuple[str, int] = None
    ):
        super().__init__(client_info=client_info)
        self._webbrowser_open = webbrowser_open or webbrowser.open
        self._authentication_timeout = timeout or self.TIMEOUT_DEFAULT
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
            log.info("Using OAuth redirect URI {u!r}".format(u=redirect_uri))

            # Build authentication URL
            auth_url = "{endpoint}?{params}".format(
                endpoint=self._provider_config['authorization_endpoint'],
                params=urllib.parse.urlencode({
                    "response_type": "code",
                    "client_id": self.client_id,
                    "scope": self._client_info.provider.get_scopes_string(),
                    "redirect_uri": redirect_uri,
                    "state": state,
                    "nonce": nonce,
                    "code_challenge": code_challenge,
                    "code_challenge_method": "S256",
                })
            )
            log.info("Sending user to auth URL {u!r}".format(u=auth_url))
            # Open browser window/tab with authentication URL
            self._webbrowser_open(auth_url)

            # TODO: show some feedback here that we are waiting browser based interaction here?

            try:
                # Collect data from redirect uri
                log.info("Waiting for request to redirect URI (timeout {t}s)".format(t=self._authentication_timeout))
                # TODO: When authentication fails (e.g. identity provider is down), this might hang the client
                #       (e.g. jupyter notebook). Is there a way to abort this? use signals? handle "abort" request?
                callbacks = list(drain_queue(
                    callback_queue,
                    initial_timeout=self._authentication_timeout,
                    on_empty=lambda **kwargs: log.info(
                        "No result yet (elapsed: {e:.2f}s)".format(e=kwargs.get("elapsed", 0))
                    )
                ))
            except TimeoutError:
                raise OidcException("Timeout: no request to redirect URI after {t}s".format(
                    t=self._authentication_timeout)
                )

        if len(callbacks) != 1:
            raise OidcException("Expected 1 OAuth redirect request, but got: {c}".format(c=len(callbacks)))

        # Parse OAuth redirect URL
        redirect_request = callbacks[0]
        log.debug("Parsing redirect request {r}".format(r=redirect_request))
        redirect_params = urllib.parse.parse_qs(urllib.parse.urlparse(redirect_request).query)
        log.debug('Parsed redirect request: {p}'.format(p=redirect_params))
        if 'state' not in redirect_params or redirect_params['state'] != [state]:
            raise OidcException("Invalid state")
        if 'code' not in redirect_params:
            raise OidcException("No auth code in redirect")
        auth_code = redirect_params["code"][0]

        return AuthCodeResult(
            auth_code=auth_code, nonce=nonce, code_verifier=code_verifier, redirect_uri=redirect_uri
        )

    def get_tokens(self) -> AccessTokenResult:
        """
        Do OpenID authentication flow with PKCE:
        get auth code and exchange for access and id token
        """
        # Get auth code from authentication provider
        auth_code_result = self._get_auth_code()

        # Exchange authentication code for access token
        result = self._do_token_post_request(post_data=dict_no_none(
            grant_type=self.grant_type,
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=auth_code_result.redirect_uri,
            code=auth_code_result.auth_code,
            code_verifier=auth_code_result.code_verifier,
        ))

        return self._get_access_token_result(result, expected_nonce=auth_code_result.nonce)


class OidcClientCredentialsAuthenticator(OidcAuthenticator):
    """
    Implementation of "Client Credentials" Flow.

    TODO: is this a useful flow in practice?
    """

    grant_type = "client_credentials"

    def _get_token_endpoint_post_data(self) -> dict:
        data = super()._get_token_endpoint_post_data()
        data["client_secret"] = self.client_secret
        return data


class OidcResourceOwnerPasswordAuthenticator(OidcAuthenticator):
    """
    Implementation of "Resource Owner Password Credentials" (ROPC) grant type.

    Note: This flow should only be used when end user owns (or highly trusts) the client code
    and the password can be handled/stored/retrieved in a secure manner.
    """

    grant_type = "password"

    def __init__(self, client_info: OidcClientInfo, username: str, password: str):
        super().__init__(client_info=client_info)
        self._username = username
        self._password = password

    def _get_token_endpoint_post_data(self) -> dict:
        data = super()._get_token_endpoint_post_data()
        data["client_secret"] = self.client_secret
        data["scope"] = self._client_info.provider.get_scopes_string()
        data["username"] = self._username
        data["password"] = self._password
        return data


class OidcRefreshTokenAuthenticator(OidcAuthenticator):
    """
    Implementation of obtaining a new OpenID Connect access token through a refresh token.
    """

    grant_type = "refresh_token"

    def __init__(self, client_info: OidcClientInfo, refresh_token: str):
        super().__init__(client_info=client_info)
        self._refresh_token = refresh_token

    def _get_token_endpoint_post_data(self) -> dict:
        data = super()._get_token_endpoint_post_data()
        if self.client_secret:
            data["client_secret"] = self.client_secret
        data["refresh_token"] = self._refresh_token
        return data


VerificationInfo = namedtuple("VerificationInfo", ["verification_uri", "device_code", "user_code", "interval"])


class OidcDeviceAuthenticator(OidcAuthenticator):
    """
    Implementation of OAuth Device Authorization grant/flow
    """

    grant_type = "urn:ietf:params:oauth:grant-type:device_code"

    def __init__(self, client_info: OidcClientInfo, display: Callable[[str], None] = print, device_code_url: str = None,
                 max_poll_time=5 * 60):
        super().__init__(client_info=client_info)
        self._display = display
        # Allow to specify/override device code URL for cases when it is not available in OIDC discovery doc.
        self._device_code_url = device_code_url or self._provider_config["device_authorization_endpoint"]
        self._max_poll_time = max_poll_time

    def _get_verification_info(self) -> VerificationInfo:
        """Get verification URL and user code"""
        resp = requests.post(
            url=self._device_code_url,
            data={"client_id": self.client_id, "scope": self._client_info.provider.get_scopes_string()}
        )
        if resp.status_code != 200:
            raise OidcException("Failed to get verification URL and user code from {u!r}: {s} {r!r} {t!r}".format(
                s=resp.status_code, r=resp.reason, u=resp.url, t=resp.text
            ))
        try:
            data = resp.json()
            return VerificationInfo(
                # Google OAuth/OIDC implementation uses non standard "verification_url" instead of "verification_uri"
                verification_uri=data["verification_uri"] if "verification_uri" in data else data["verification_url"],
                device_code=data["device_code"],
                user_code=data["user_code"],
                interval=data.get("interval", 5),
            )
        except Exception as e:
            raise OidcException("Failed to parse device authorization request: {e!r}".format(e=e))

    def get_tokens(self) -> AccessTokenResult:
        # Get verification url and user code
        verification_info = self._get_verification_info()
        self._display("To authenticate: visit {u} and enter the user code {c!r}.".format(
            u=verification_info.verification_uri, c=verification_info.user_code)
        )

        # Poll token endpoint
        elapsed = create_timer()
        token_endpoint = self._provider_config['token_endpoint']
        post_data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "device_code": verification_info.device_code,
            "grant_type": self.grant_type
        }
        poll_interval = verification_info.interval
        while elapsed() <= self._max_poll_time:
            time.sleep(poll_interval)

            log.debug("Doing {g!r} token request {u!r} with post data fields {p!r} (client_id {c!r})".format(
                g=self.grant_type, c=self.client_id, u=token_endpoint, p=list(post_data.keys()))
            )
            resp = requests.post(url=token_endpoint, data=post_data)
            if resp.status_code == 200:
                log.info("[{e:5.1f}s] Authorized successfully.".format(e=elapsed()))
                self._display("Authorized successfully.")
                return self._get_access_token_result(data=resp.json())
            else:
                try:
                    error = resp.json()["error"]
                except Exception:
                    error = "unknown"
                if error == "authorization_pending":
                    log.info("[{e:5.1f}s] Authorization pending.".format(e=elapsed()))
                elif error == "slow_down":
                    log.info("[{e:5.1f}s] Polling too fast, will slow down.".format(e=elapsed()))
                    poll_interval += 5
                else:
                    raise OidcException("Failed to retrieve access token at {u!r}: {s} {r!r} {t!r}".format(
                        s=resp.status_code, r=resp.reason, u=token_endpoint, t=resp.text
                    ))

        raise OidcException("Timeout exceeded {m:.1f}s while polling for access token at {u!r}".format(
            u=token_endpoint, m=self._max_poll_time
        ))

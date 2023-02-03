"""
This module provides a Connection object to manage and persist settings when interacting with the OpenEO API.
"""
import datetime
import json
import logging
import shlex
import sys
import warnings
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Tuple, Union, Callable, Optional, Any, Iterator
from urllib.parse import urljoin

import requests
from requests import Response
from requests.auth import HTTPBasicAuth, AuthBase

import openeo
from openeo.capabilities import ApiVersionException, ComparableVersion
from openeo.config import get_config_option, config_log
from openeo.internal.graph_building import PGNode, as_flat_graph
from openeo.internal.jupyter import VisualDict, VisualList
from openeo.internal.processes.builder import ProcessBuilderBase
from openeo.internal.warnings import legacy_alias, deprecated
from openeo.metadata import CollectionMetadata, SpatialDimension, TemporalDimension, BandDimension, Band
from openeo.rest import OpenEoClientException, OpenEoApiError, OpenEoRestError
from openeo.rest.auth.auth import NullAuth, BearerAuth, BasicBearerAuth, OidcBearerAuth, OidcRefreshInfo
from openeo.rest.auth.config import RefreshTokenStore, AuthConfig
from openeo.rest.auth.oidc import OidcClientCredentialsAuthenticator, OidcAuthCodePkceAuthenticator, \
    OidcClientInfo, OidcAuthenticator, OidcRefreshTokenAuthenticator, OidcResourceOwnerPasswordAuthenticator, \
    OidcDeviceAuthenticator, OidcProviderInfo, OidcException, DefaultOidcClientGrant, GrantsChecker
from openeo.rest.datacube import DataCube
from openeo.rest.imagecollectionclient import ImageCollectionClient
from openeo.rest.mlmodel import MlModel
from openeo.rest.job import BatchJob, RESTJob
from openeo.rest.rest_capabilities import RESTCapabilities
from openeo.rest.service import Service
from openeo.rest.udp import RESTUserDefinedProcess, Parameter
from openeo.rest.vectorcube import VectorCube
from openeo.util import (
    ensure_list,
    dict_no_none,
    rfc3339,
    load_json_resource,
    LazyLoadCache,
    ContextTimer,
    str_truncate,
    url_join,
)

_log = logging.getLogger(__name__)


class RestApiConnection:
    """Base connection class implementing generic REST API request functionality"""

    def __init__(
            self, root_url: str, auth: AuthBase = None, session: requests.Session = None,
            default_timeout: Optional[int] = None, slow_response_threshold: Optional[float] = None,
    ):
        self._root_url = root_url
        self.auth = auth or NullAuth()
        self.session = session or requests.Session()
        self.default_timeout = default_timeout
        self.default_headers = {
            "User-Agent": "openeo-python-client/{cv} {py}/{pv} {pl}".format(
                cv=openeo.client_version(),
                py=sys.implementation.name, pv=".".join(map(str, sys.version_info[:3])),
                pl=sys.platform
            )
        }
        self.slow_response_threshold = slow_response_threshold

    @property
    def root_url(self):
        return self._root_url

    def build_url(self, path: str):
        return url_join(self._root_url, path)

    def _merged_headers(self, headers: dict) -> dict:
        """Merge default headers with given headers"""
        result = self.default_headers.copy()
        if headers:
            result.update(headers)
        return result

    def _is_external(self, url: str) -> bool:
        """Check if given url is external (not under root url)"""
        root = self.root_url.rstrip("/")
        return not (url == root or url.startswith(root + '/'))

    def request(self, method: str, path: str, headers: dict = None, auth: AuthBase = None,
                check_error=True, expected_status=None, **kwargs):
        """Generic request send"""
        url = self.build_url(path)
        # Don't send default auth headers to external domains.
        auth = auth or (self.auth if not self._is_external(url) else None)
        slow_response_threshold = kwargs.pop("slow_response_threshold", self.slow_response_threshold)
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug("Request `{m} {u}` with headers {h}, auth {a}, kwargs {k}".format(
                m=method.upper(), u=url, h=headers and headers.keys(), a=type(auth).__name__, k=list(kwargs.keys()))
            )
        with ContextTimer() as timer:
            resp = self.session.request(
                method=method,
                url=url,
                headers=self._merged_headers(headers),
                auth=auth,
                timeout=kwargs.pop("timeout", self.default_timeout),
                **kwargs
            )
        if slow_response_threshold and timer.elapsed() > slow_response_threshold:
            _log.warning("Slow response: `{m} {u}` took {e:.2f}s (>{t:.2f}s)".format(
                m=method.upper(), u=str_truncate(url, width=64),
                e=timer.elapsed(), t=slow_response_threshold
            ))
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug("Got {r} headers {h!r}".format(r=resp, h=resp.headers))
        # Check for API errors and unexpected HTTP status codes as desired.
        status = resp.status_code
        expected_status = ensure_list(expected_status) if expected_status else []
        if check_error and status >= 400 and status not in expected_status:
            self._raise_api_error(resp)
        if expected_status and status not in expected_status:
            raise OpenEoRestError("Got status code {s!r} for `{m} {p}` (expected {e!r})".format(
                m=method.upper(), p=path, s=status, e=expected_status)
            )
        return resp

    def _raise_api_error(self, response: requests.Response):
        """Convert API error response to Python exception"""
        status_code = response.status_code
        try:
            # Try parsing the error info according to spec and wrap it in an exception.
            info = response.json()
            exception = OpenEoApiError(
                http_status_code=status_code,
                code=info.get("code", "unknown"),
                message=info.get("message", "unknown error"),
                id=info.get("id"),
                url=info.get("url"),
            )
        except Exception:
            # Parsing of error info went wrong: let's see if we can still extract some helpful information.
            text = response.text
            _log.warning("Failed to parse API error response: {s} {t!r}".format(s=status_code, t=text))
            if status_code == 502 and "Proxy Error" in text:
                msg = "Received 502 Proxy Error." \
                      " This typically happens if an OpenEO request takes too long and is killed." \
                      " Consider using batch jobs instead of doing synchronous processing."
                exception = OpenEoApiError(http_status_code=status_code, message=msg)
            else:
                exception = OpenEoApiError(http_status_code=status_code, message=text)
        raise exception

    def get(self, path, stream=False, auth: AuthBase = None, **kwargs) -> Response:
        """
        Do GET request to REST API.

        :param path: API path (without root url)
        :param stream: True if the get request should be streamed, else False
        :param auth: optional custom authentication to use instead of the default one
        :return: response: Response
        """
        return self.request("get", path=path, stream=stream, auth=auth, **kwargs)

    def post(self, path, json: dict = None, **kwargs) -> Response:
        """
        Do POST request to REST API.

        :param path: API path (without root url)
        :param json: Data (as dictionary) to be posted with JSON encoding)
        :return: response: Response
        """
        return self.request("post", path=path, json=json, allow_redirects=False, **kwargs)

    def delete(self, path, **kwargs) -> Response:
        """
        Do DELETE request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("delete", path=path, allow_redirects=False, **kwargs)

    def patch(self, path, **kwargs) -> Response:
        """
        Do PATCH request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("patch", path=path, allow_redirects=False, **kwargs)

    def put(self, path, headers: dict = None, data=None, **kwargs) -> Response:
        """
        Do PUT request to REST API.

        :param path: API path (without root url)
        :param headers: headers that gets added to the request.
        :param data: data that gets added to the request.
        :return: response: Response
        """
        return self.request("put", path=path, data=data, headers=headers, allow_redirects=False, **kwargs)

    def __repr__(self):
        return "<{c} to {r!r} with {a}>".format(c=type(self).__name__, r=self._root_url, a=type(self.auth).__name__)


class Connection(RestApiConnection):
    """
    Connection to an openEO backend.
    """

    _MINIMUM_API_VERSION = ComparableVersion("0.4.0")

    def __init__(
            self, url: str, auth: AuthBase = None, session: requests.Session = None, default_timeout: int = None,
            auth_config: AuthConfig = None, refresh_token_store: RefreshTokenStore = None,
            slow_response_threshold: Optional[float] = None,
    ):
        """
        Constructor of Connection, authenticates user.

        :param url: String Backend root url
        """
        if "://" not in url:
            url = "https://" + url
        self._orig_url = url
        super().__init__(
            root_url=self.version_discovery(url, session=session, timeout=default_timeout),
            auth=auth, session=session, default_timeout=default_timeout,
            slow_response_threshold=slow_response_threshold,
        )
        self._capabilities_cache = LazyLoadCache()

        # Initial API version check.
        if self._api_version.below(self._MINIMUM_API_VERSION):
            raise ApiVersionException("OpenEO API version should be at least {m!s}, but got {v!s}".format(
                m=self._MINIMUM_API_VERSION, v=self._api_version)
            )

        self._auth_config = auth_config
        self._refresh_token_store = refresh_token_store

    @classmethod
    def version_discovery(cls, url: str, session: requests.Session = None, timeout: Optional[int] = None) -> str:
        """
        Do automatic openEO API version discovery from given url, using a "well-known URI" strategy.

        :param url: initial backend url (not including "/.well-known/openeo")
        :return: root url of highest supported backend version
        """
        try:
            connection = RestApiConnection(url, session=session)
            well_known_url_response = connection.get("/.well-known/openeo", timeout=timeout)
            assert well_known_url_response.status_code == 200
            versions = well_known_url_response.json()["versions"]
            supported_versions = [v for v in versions if cls._MINIMUM_API_VERSION <= v["api_version"]]
            assert supported_versions
            production_versions = [v for v in supported_versions if v.get("production", True)]
            highest_version = max(production_versions or supported_versions, key=lambda v: v["api_version"])
            _log.debug("Highest supported version available in backend: %s" % highest_version)
            return highest_version['url']
        except Exception:
            # Be very lenient about failing on the well-known URI strategy.
            return url

    def _get_auth_config(self) -> AuthConfig:
        if self._auth_config is None:
            self._auth_config = AuthConfig()
        return self._auth_config

    def _get_refresh_token_store(self) -> RefreshTokenStore:
        if self._refresh_token_store is None:
            self._refresh_token_store = RefreshTokenStore()
        return self._refresh_token_store

    def authenticate_basic(self, username: str = None, password: str = None) -> 'Connection':
        """
        Authenticate a user to the backend using basic username and password.

        :param username: User name
        :param password: User passphrase
        """
        if not self.capabilities().supports_endpoint("/credentials/basic", method="GET"):
            raise OpenEoClientException("This openEO back-end does not support basic authentication.")
        if username is None:
            username, password = self._get_auth_config().get_basic_auth(backend=self._orig_url)
            if username is None:
                raise OpenEoClientException("No username/password given or found.")

        resp = self.get(
            '/credentials/basic',
            # /credentials/basic is the only endpoint that expects a Basic HTTP auth
            auth=HTTPBasicAuth(username, password)
        ).json()
        # Switch to bearer based authentication in further requests.
        if self._api_version.at_least("1.0.0"):
            self.auth = BasicBearerAuth(access_token=resp["access_token"])
        else:
            self.auth = BearerAuth(bearer=resp["access_token"])
        return self

    def _get_oidc_provider(self, provider_id: Union[str, None] = None) -> Tuple[str, OidcProviderInfo]:
        """
        Get OpenID Connect discovery URL for given provider_id

        :param provider_id: id of OIDC provider as specified by backend (/credentials/oidc).
            Can be None if there is just one provider.
        :return: updated provider_id and provider info object
        """
        if self._api_version.at_least("1.0.0"):
            oidc_info = self.get("/credentials/oidc", expected_status=200).json()
            providers = OrderedDict((p["id"], p) for p in oidc_info["providers"])
            if len(providers) < 1:
                raise OpenEoClientException("Backend lists no OIDC providers.")
            _log.info("Found OIDC providers: {p}".format(p=list(providers.keys())))
            if provider_id:
                if provider_id not in providers:
                    raise OpenEoClientException(
                        "Requested OIDC provider {r!r} not available. Should be one of {p}.".format(
                            r=provider_id, p=list(providers.keys())
                        )
                    )
                provider = providers[provider_id]
            elif len(providers) == 1:
                provider_id, provider = providers.popitem()
                _log.info("No OIDC provider given, but only one available: {p!r}. Using that one.".format(
                    p=provider_id
                ))
            else:
                # Check if there is a single provider in the config to use.
                backend = self._orig_url
                provider_configs = self._get_auth_config().get_oidc_provider_configs(backend=backend)
                intersection = set(provider_configs.keys()).intersection(providers.keys())
                if len(intersection) == 1:
                    provider_id = intersection.pop()
                    provider = providers[provider_id]
                    _log.info(
                        "No OIDC provider given, but only one in config (for backend {b!r}): {p!r}."
                        " Using that one.".format(b=backend, p=provider_id)
                    )
                else:
                    provider_id, provider = providers.popitem(last=False)
                    _log.info("No OIDC provider given. Using first provider {p!r} as advertised by backend.".format(
                        p=provider_id
                    ))
            provider = OidcProviderInfo.from_dict(provider)
        else:
            # Per spec: '/credentials/oidc' will redirect to  OpenID Connect discovery document
            provider = OidcProviderInfo(discovery_url=self.build_url('/credentials/oidc'))
        return provider_id, provider

    def _get_oidc_provider_and_client_info(
            self, provider_id: str,
            client_id: Union[str, None], client_secret: Union[str, None],
            default_client_grant_check: Union[None, GrantsChecker] = None
    ) -> Tuple[str, OidcClientInfo]:
        """
        Resolve provider_id and client info (as given or from config)

        :param provider_id: id of OIDC provider as specified by backend (/credentials/oidc).
            Can be None if there is just one provider.

        :return: OIDC provider id and client info
        """
        provider_id, provider = self._get_oidc_provider(provider_id)

        if client_id is None:
            _log.debug("No client_id: checking config for preferred client_id")
            client_id, client_secret = self._get_auth_config().get_oidc_client_configs(
                backend=self._orig_url, provider_id=provider_id
            )
            if client_id:
                _log.info("Using client_id {c!r} from config (provider {p!r})".format(c=client_id, p=provider_id))
        if client_id is None and default_client_grant_check:
            # Try "default_clients" from backend's provider info.
            _log.debug("No client_id given: checking default clients in backend's provider info")
            client_id = provider.get_default_client_id(grant_check=default_client_grant_check)
            if client_id:
                _log.info("Using default client_id {c!r} from OIDC provider {p!r} info.".format(
                    c=client_id, p=provider_id
                ))
        if client_id is None:
            raise OpenEoClientException("No client_id found.")

        client_info = OidcClientInfo(client_id=client_id, client_secret=client_secret, provider=provider)

        return provider_id, client_info

    def _authenticate_oidc(
            self,
            authenticator: OidcAuthenticator,
            *,
            provider_id: str,
            store_refresh_token: bool = False,
            fallback_refresh_token_to_store: Optional[str] = None,
            refreshable: bool = False,
    ) -> 'Connection':
        """
        Authenticate through OIDC and set up bearer token (based on OIDC access_token) for further requests.
        """
        tokens = authenticator.get_tokens(request_refresh_token=store_refresh_token)
        _log.info("Obtained tokens: {t}".format(t=[k for k, v in tokens._asdict().items() if v]))
        if store_refresh_token:
            refresh_token = tokens.refresh_token or fallback_refresh_token_to_store
            if refresh_token:
                self._get_refresh_token_store().set_refresh_token(
                    issuer=authenticator.provider_info.issuer,
                    client_id=authenticator.client_id,
                    refresh_token=refresh_token
                )
                refreshable = True
            else:
                _log.warning("No OIDC refresh token to store.")
        token = tokens.access_token
        if self._api_version.at_least("1.0.0"):
            if refreshable:
                refresh_data = OidcRefreshInfo(
                    provider_id=provider_id,
                    client_id=authenticator.client_id,
                )
            else:
                refresh_data = None
            self.auth = OidcBearerAuth(provider_id=provider_id, access_token=token, refresh_data=refresh_data)
        else:
            self.auth = BearerAuth(bearer=token)
        return self

    def authenticate_oidc_authorization_code(
            self,
            client_id: str = None,
            client_secret: str = None,
            provider_id: str = None,
            timeout: int = None,
            server_address: Tuple[str, int] = None,
            webbrowser_open: Callable = None,
            store_refresh_token=False,
    ) -> 'Connection':
        """
        OpenID Connect Authorization Code Flow (with PKCE).
        """
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret,
            default_client_grant_check=[DefaultOidcClientGrant.AUTH_CODE_PKCE],
        )
        authenticator = OidcAuthCodePkceAuthenticator(
            client_info=client_info,
            webbrowser_open=webbrowser_open, timeout=timeout, server_address=server_address
        )
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc_client_credentials(
            self,
            client_id: str = None,
            client_secret: str = None,
            provider_id: str = None,
            store_refresh_token=False,
    ) -> 'Connection':
        """
        OpenID Connect Client Credentials flow.
        """
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret
        )
        authenticator = OidcClientCredentialsAuthenticator(client_info=client_info)
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc_resource_owner_password_credentials(
            self,
            username: str, password: str,
            client_id: str = None,
            client_secret: str = None,
            provider_id: str = None,
            store_refresh_token=False
    ) -> 'Connection':
        """
        OpenId Connect Resource Owner Password Credentials
        """
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret
        )
        # TODO: also get username and password from config?
        authenticator = OidcResourceOwnerPasswordAuthenticator(
            client_info=client_info, username=username, password=password
        )
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc_refresh_token(
            self, client_id: str = None, refresh_token: str = None, client_secret: str = None, provider_id: str = None,
            store_refresh_token=False,
    ) -> 'Connection':
        """
        OpenId Connect Refresh Token
        """
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret,
            default_client_grant_check=[DefaultOidcClientGrant.REFRESH_TOKEN],
        )

        if refresh_token is None:
            refresh_token = self._get_refresh_token_store().get_refresh_token(
                issuer=client_info.provider.issuer,
                client_id=client_info.client_id
            )
            if refresh_token is None:
                raise OpenEoClientException("No refresh token given or found")

        authenticator = OidcRefreshTokenAuthenticator(client_info=client_info, refresh_token=refresh_token)
        return self._authenticate_oidc(
            authenticator,
            provider_id=provider_id,
            store_refresh_token=store_refresh_token,
            fallback_refresh_token_to_store=refresh_token,
            refreshable=True,
        )

    def authenticate_oidc_device(
            self, client_id: str = None, client_secret: str = None, provider_id: str = None,
            store_refresh_token=False, use_pkce: Union[bool, None] = None,
            **kwargs
    ) -> 'Connection':
        """
        Authenticate with OAuth Device Authorization grant/flow

        :param use_pkce: Use PKCE instead of client secret.
            If not set explicitly to `True` (use PKCE) or `False` (use client secret),
            it will be attempted to detect the best mode automatically.
            Note that PKCE for device code is not widely supported among OIDC providers.

        .. versionchanged:: 0.5.1 Add :py:obj:`use_pkce` argument
        """
        _g = DefaultOidcClientGrant  # alias for compactness
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret,
            default_client_grant_check=(lambda grants: _g.DEVICE_CODE in grants or _g.DEVICE_CODE_PKCE in grants),
        )
        authenticator = OidcDeviceAuthenticator(client_info=client_info, use_pkce=use_pkce, **kwargs)
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc(
            self,
            provider_id: str = None,
            client_id: Union[str, None] = None, client_secret: Union[str, None] = None,
            store_refresh_token: bool = True,
            use_pkce: Union[bool, None] = None,
    ):
        """
        Do OpenID Connect authentication, first trying refresh tokens and falling back on device code flow.

        .. versionadded:: 0.6.0
        """
        _g = DefaultOidcClientGrant  # alias for compactness
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret,
            default_client_grant_check=lambda grants: (
                    _g.REFRESH_TOKEN in grants and (_g.DEVICE_CODE in grants or _g.DEVICE_CODE_PKCE in grants)
            )
        )

        # Try refresh token first.
        refresh_token = self._get_refresh_token_store().get_refresh_token(
            issuer=client_info.provider.issuer,
            client_id=client_info.client_id
        )
        if refresh_token:
            try:
                _log.info("Found refresh token: trying refresh token based authentication.")
                authenticator = OidcRefreshTokenAuthenticator(client_info=client_info, refresh_token=refresh_token)
                con = self._authenticate_oidc(
                    authenticator,
                    provider_id=provider_id,
                    store_refresh_token=store_refresh_token,
                    fallback_refresh_token_to_store=refresh_token,
                )
                # TODO: pluggable/jupyter-aware display function?
                print("Authenticated using refresh token.")
                return con
            except OidcException as e:
                _log.info("Refresh token based authentication failed: {e}.".format(e=e))

        # Fall back on device code flow
        # TODO: make it possible to do other fallback flows too?
        _log.info("Trying device code flow.")
        authenticator = OidcDeviceAuthenticator(client_info=client_info, use_pkce=use_pkce)
        con = self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)
        print("Authenticated using device code flow.")
        return con

    def request(
            self, method: str, path: str, headers: dict = None, auth: AuthBase = None,
            check_error=True, expected_status=None, **kwargs,
    ):
        # Do request, but with retry when access token has expired and refresh token is available.
        def _request():
            return super(Connection, self).request(
                method=method, path=path, headers=headers, auth=auth,
                check_error=check_error, expected_status=expected_status, **kwargs,
            )

        try:
            # Initial request attempt
            return _request()
        except OpenEoApiError as api_exc:
            if api_exc.http_status_code == 403 and api_exc.code == "TokenInvalid":
                # Auth token expired: can we refresh?
                if isinstance(self.auth, OidcBearerAuth) and self.auth.refresh_data:
                    _log.debug(
                        f"Back-end request failed with {str(api_exc)!r}."
                        f" Trying to re-authenticate with the refresh token."
                    )
                    try:
                        self.authenticate_oidc_refresh_token(
                            client_id=self.auth.refresh_data.client_id,
                            provider_id=self.auth.refresh_data.provider_id,
                        )
                        _log.warning(
                            f"Connection with expired access token ([{api_exc.http_status_code}] {api_exc.code})"
                            f" automatically re-authenticated with refresh token."
                        )
                    except OpenEoClientException as auth_exc:
                        _log.error(
                            f"Connection with expired access token ([{api_exc.http_status_code}] {api_exc.code})"
                            f" failed to automatically re-authenticate using refresh token: {auth_exc!r}.")
                    else:
                        # Retry request.
                        return _request()
            raise

    def describe_account(self) -> str:
        """
        Describes the currently authenticated user account.
        """
        return self.get('/me', expected_status=200).json()

    @deprecated("use :py:meth:`list_jobs` instead", version="0.4.10")
    def user_jobs(self) -> dict:
        return self.list_jobs()

    def list_collections(self) -> List[dict]:
        """
        List basic metadata of all collections provided by the back-end.

        .. caution::

            Only the basic collection metadata will be returned.
            To obtain full metadata of a particular collection,
            it is recommended to use :py:meth:`~openeo.rest.connection.Connection.describe_collection` instead.

        :return: list of dictionaries with basic collection metadata.
        """
        data = self.get('/collections', expected_status=200).json()["collections"]
        return VisualList("collections", data=data)

    def list_collection_ids(self) -> List[str]:
        """
        List all collection ids provided by the back-end.

        .. seealso::

            :py:meth:`~openeo.rest.connection.Connection.describe_collection`
            to get the metadata of a particular collection.

        :return: list of collection ids
        """
        return [collection['id'] for collection in self.list_collections() if 'id' in collection]

    def capabilities(self) -> RESTCapabilities:
        """
        Loads all available capabilities.
        """
        return self._capabilities_cache.get(
            "capabilities",
            load=lambda: RESTCapabilities(data=self.get('/', expected_status=200).json(), url=self._orig_url)
        )

    def list_output_formats(self) -> dict:
        if self._api_version.at_least("1.0.0"):
            return self.list_file_formats()["output"]
        else:
            return self.get('/output_formats', expected_status=200).json()

    list_file_types = legacy_alias(list_output_formats, "list_file_types")

    def list_file_formats(self) -> dict:
        """
        Get available input and output formats
        """
        formats = self._capabilities_cache.get(
            key="file_formats",
            load=lambda: self.get('/file_formats', expected_status=200).json()
        )
        return VisualDict("file-formats", data=formats)

    def list_service_types(self) -> dict:
        """
        Loads all available service types.

        :return: data_dict: Dict All available service types
        """
        types = self._capabilities_cache.get(
            key="service_types",
            load=lambda: self.get('/service_types', expected_status=200).json()
        )
        return VisualDict("service-types", data=types)

    def list_udf_runtimes(self) -> dict:
        """
        Loads all available UDF runtimes.

        :return: data_dict: Dict All available UDF runtimes
        """
        runtimes = self._capabilities_cache.get(
            key="udf_runtimes",
            load=lambda: self.get('/udf_runtimes', expected_status=200).json()
        )
        return VisualDict("udf-runtimes", data=runtimes)

    def list_services(self) -> dict:
        """
        Loads all available services of the authenticated user.

        :return: data_dict: Dict All available services
        """
        # TODO return parsed service objects
        services = self.get('/services', expected_status=200).json()["services"]
        return VisualList("data-table", data=services, parameters={'columns': 'services'})

    def describe_collection(self, collection_id: str) -> dict:
        """
        Get full collection metadata for given collection id.

        .. seealso::

            :py:meth:`~openeo.rest.connection.Connection.list_collection_ids`
            to list all collection ids provided by the back-end.

        :param collection_id: collection id
        :return: collection metadata.
        """
        # TODO: duplication with `Connection.collection_metadata`: deprecate one or the other?
        data = self.get(f"/collections/{collection_id}", expected_status=200).json()
        return VisualDict("collection", data=data)

    def collection_items(self, name, spatial_extent: Optional[List[float]] = None, temporal_extent: Optional[List[Union[str, datetime.datetime]]] = None, limit: int = None) -> Iterator[dict]:
        """
        Loads items for a specific image collection.
        May not be available for all collections.

        This is an experimental API and is subject to change.

        :param name: String Id of the collection
        :param spatial_extent: Limits the items to the given bounding box in WGS84:
            1. Lower left corner, coordinate axis 1
            2. Lower left corner, coordinate axis 2
            3. Upper right corner, coordinate axis 1
            4. Upper right corner, coordinate axis 2

        :param temporal_extent: Limits the items to the specified temporal interval.
        :param limit: The amount of items per request/page. If None, the back-end decides.
            The interval has to be specified as an array with exactly two elements (start, end).
            Also supports open intervals by setting one of the boundaries to None, but never both.

        :return: data_list: List A list of items
        """
        url = '/collections/{}/items'.format(name)
        params = {}
        if spatial_extent:
            params["bbox"] = ",".join(str(c) for c in spatial_extent)
        if temporal_extent:
            params["datetime"] = "/".join(".." if t is None else rfc3339.normalize(t) for t in temporal_extent)
        if limit is not None and limit > 0:
            params['limit'] = limit

        return paginate(self, url, params, lambda response, page: VisualDict("items", data = response, parameters = {'show-map': True, 'heading': 'Page {} - Items'.format(page)}))

    def collection_metadata(self, name) -> CollectionMetadata:
        # TODO: duplication with `Connection.describe_collection`: deprecate one or the other?
        return CollectionMetadata(metadata=self.describe_collection(name))

    def list_processes(self, namespace: str = None) -> List[dict]:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        """
        Loads all available processes of the back end.

        :param namespace: The namespace for which to list processes.

        :return: processes_dict: Dict All available processes of the back end.
        """
        if namespace is None:
            processes = self._capabilities_cache.get(
                key=("processes", "backend"),
                load=lambda: self.get('/processes', expected_status=200).json()["processes"]
            )
        else:
            processes = self.get('/processes/' + namespace, expected_status=200).json()["processes"]
        return VisualList("processes", data=processes, parameters={'show-graph': True, 'provide-download': False})

    def describe_process(self, id: str, namespace: str = None) -> dict:
        """
        Returns a single process from the back end.

        :param id: The id of the process.
        :param namespace: The namespace of the process.

        :return: The process definition.
        """

        processes = self.list_processes(namespace)
        for process in processes:
            if process["id"] == id:
                return VisualDict("process", data=process, parameters={'show-graph': True, 'provide-download': False})

        raise OpenEoClientException("Process does not exist.")


    def list_jobs(self) -> List[dict]:
        """
        Lists all jobs of the authenticated user.

        :return: job_list: Dict of all jobs of the user.
        """
        # TODO: Parse the result so that there get Job classes returned?
        resp = self.get('/jobs', expected_status=200).json()
        if resp.get("federation:missing"):
            _log.warning("Partial user job listing due to missing federation components: {c}".format(
                c=",".join(resp["federation:missing"])
            ))
        jobs = resp["jobs"]
        return VisualList("data-table", data=jobs, parameters={'columns': 'jobs'})

    def save_user_defined_process(
            self, user_defined_process_id: str,
            process_graph: Union[dict, ProcessBuilderBase],
            parameters: List[Union[dict, Parameter]] = None,
            public: bool = False,
            summary: Optional[str] = None,
            description: Optional[str] = None,
            returns: Optional[dict] = None,
            categories: Optional[List[str]] = None,
            examples: Optional[List[dict]] = None,
            links: Optional[List[dict]] = None,
    ) -> RESTUserDefinedProcess:
        """
        Store a process graph and its metadata on the backend as a user-defined process for the authenticated user.

        :param user_defined_process_id: unique identifier for the user-defined process
        :param process_graph: a process graph
        :param parameters: a list of parameters
        :param public: visible to other users?
        :param summary: A short summary of what the process does.
        :param description: Detailed description to explain the entity. CommonMark 0.29 syntax MAY be used for rich text representation.
        :param returns: Description and schema of the return value.
        :param categories: A list of categories.
        :param examples: A list of examples.
        :param links: A list of links.
        :return: a RESTUserDefinedProcess instance
        """
        if user_defined_process_id in set(p["id"] for p in self.list_processes()):
            warnings.warn("Defining user-defined process {u!r} with same id as a pre-defined process".format(
                u=user_defined_process_id))
        if not parameters:
            warnings.warn("Defining user-defined process {u!r} without parameters".format(u=user_defined_process_id))
        udp = RESTUserDefinedProcess(user_defined_process_id=user_defined_process_id, connection=self)
        udp.store(
            process_graph=process_graph, parameters=parameters, public=public,
            summary=summary, description=description,
            returns=returns, categories=categories, examples=examples, links=links
        )
        return udp

    def list_user_defined_processes(self) -> List[dict]:
        """
        Lists all user-defined processes of the authenticated user.
        """
        data = self.get("/process_graphs", expected_status=200).json()["processes"]
        return VisualList("processes", data=data, parameters={'show-graph': True, 'provide-download': False})

    def user_defined_process(self, user_defined_process_id: str) -> RESTUserDefinedProcess:
        """
        Get the user-defined process based on its id. The process with the given id should already exist.

        :param user_defined_process_id: the id of the user-defined process
        :return: a RESTUserDefinedProcess instance
        """
        return RESTUserDefinedProcess(user_defined_process_id=user_defined_process_id, connection=self)

    def validate_process_graph(self, process_graph: dict) -> List[dict]:
        """
        Validate a process graph without executing it.

        :param process_graph: (flat) dict representing process graph
        :return: list of errors (dictionaries with "code" and "message" fields)
        """
        request = {"process_graph": process_graph}
        return self.post(path="/validation", json=request, expected_status=200).json()["errors"]

    @property
    def _api_version(self) -> ComparableVersion:
        # TODO make this a public property (it's also useful outside the Connection class)
        return self.capabilities().api_version_check

    def vectorcube_from_paths(
        self, paths: List[str], format: str, options: dict = {}
    ) -> VectorCube:
        """
        Loads one or more files referenced by url or path that is accessible by the backend.

        :param paths: The files to read.
        :param format:  The file format to read from. It must be one of the values that the server reports as supported input file formats.
        :param options: The file format parameters to be used to read the files. Must correspond to the parameters that the server reports as supported parameters for the chosen format.

        :return: A :py:class:`VectorCube`.

        .. versionadded:: 0.14.0
        """
        graph = PGNode(
            "load_uploaded_files",
            arguments=dict(paths=paths, format=format, options=options),
        )
        return VectorCube(graph=graph, connection=self)

    def datacube_from_process(self, process_id: str, namespace: str = None, **kwargs) -> DataCube:
        """
        Load a data cube from a (custom) process.

        :param process_id: The process id.
        :param namespace: optional: process namespace
        :param kwargs: The arguments of the custom process
        :return: A :py:class:`DataCube`, without valid metadata, as the client is not aware of this custom process.
        """

        if self._api_version.at_least("1.0.0"):
            graph = PGNode(process_id, namespace=namespace, arguments=kwargs)
            return DataCube(graph=graph, connection=self)
        else:
            raise OpenEoClientException(
                "This method requires support for at least version 1.0.0 in the openEO backend.")

    def datacube_from_flat_graph(self, flat_graph: dict, parameters: dict = None) -> DataCube:
        """
        Construct a :py:class:`DataCube` from a flat dictionary representation of a process graph.

        :param flat_graph: flat dictionary representation of a process graph
            or a process dictionary with such a flat process graph under a "process_graph" field
            (and optionally parameter metadata under a "parameters" field).
        :return: A :py:class:`DataCube` corresponding with the operations encoded in the process graph
        """
        if self._api_version.below("1.0.0"):
            raise OpenEoClientException(
                "This method requires support for at least version 1.0.0 in the openEO backend.")

        parameters = parameters or {}

        if "process_graph" in flat_graph:
            # `flat_graph` is a "process" structure
            # Extract defaults from declared parameters.
            for param in flat_graph.get("parameters") or []:
                if "default" in param:
                    parameters.setdefault(param["name"], param["default"])

            flat_graph = flat_graph["process_graph"]

        pgnode = PGNode.from_flat_graph(flat_graph=flat_graph, parameters=parameters or {})
        return DataCube(graph=pgnode, connection=self)

    def datacube_from_json(self, src: Union[str, Path], parameters: dict = None) -> DataCube:
        """
        Construct a :py:class:`DataCube` from JSON resource containing (flat) process graph representation.

        :param src: raw JSON string, URL to JSON resource or path to local JSON file
        :return: A :py:class:`DataCube` corresponding with the operations encoded in the process graph
        """
        return self.datacube_from_flat_graph(load_json_resource(src), parameters=parameters)

    def load_collection(
            self,
            collection_id: str,
            spatial_extent: Optional[Dict[str, float]] = None,
            temporal_extent: Optional[List[Union[str, datetime.datetime, datetime.date]]] = None,
            bands: Optional[List[str]] = None,
            properties: Optional[Dict[str, Union[str, PGNode, Callable]]] = None,
            max_cloud_cover: Optional[float] = None,
            fetch_metadata=True,
    ) -> DataCube:
        """
        Load a DataCube by collection id.

        :param collection_id: image collection identifier
        :param spatial_extent: limit data to specified bounding box or polygons
        :param temporal_extent: limit data to specified temporal interval
        :param bands: only add the specified bands
        :param properties: limit data by metadata property predicates
        :param max_cloud_cover: shortcut to set maximum cloud cover ("eo:cloud_cover" collection property)
        :return: a datacube containing the requested data

        .. versionadded:: 0.13.0
            added the ``max_cloud_cover`` argument.
        """
        if self._api_version.at_least("1.0.0"):
            return DataCube.load_collection(
                collection_id=collection_id, connection=self,
                spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=bands, properties=properties,
                max_cloud_cover=max_cloud_cover,
                fetch_metadata=fetch_metadata,
            )
        else:
            return ImageCollectionClient.load_collection(
                collection_id=collection_id, session=self,
                spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=bands
            )

    imagecollection = legacy_alias(load_collection, name="imagecollection")

    def load_result(
            self,
            id: str,
            spatial_extent: Optional[Dict[str, float]] = None,
            temporal_extent: Optional[List[Union[str, datetime.datetime, datetime.date]]] = None,
            bands: Optional[List[str]] = None,
    ) -> DataCube:
        """
        Loads batch job results by job id from the server-side user workspace.
        The job must have been stored by the authenticated user on the back-end currently connected to.

        :param id: The id of a batch job with results.
        :param spatial_extent: limit data to specified bounding box or polygons
        :param temporal_extent: limit data to specified temporal interval
        :param bands: only add the specified bands

        :return: a :py:class:`DataCube`
        """
        # TODO: add check that back-end supports `load_result` process?
        if self._api_version.below("1.0.0"):
            raise OpenEoClientException(
                "This method requires support for at least version 1.0.0 in the openEO backend.")
        metadata = CollectionMetadata({}, dimensions=[
            SpatialDimension(name="x", extent=[]),
            SpatialDimension(name="y", extent=[]),
            TemporalDimension(name='t', extent=[]),
            BandDimension(name="bands", bands=[Band("unknown")]),
        ])
        cube = self.datacube_from_process(process_id="load_result", id=id,
                                             **dict_no_none(spatial_extent=spatial_extent,
                                                            temporal_extent=temporal_extent and DataCube._get_temporal_extent(
                                                                temporal_extent), bands=bands))
        cube.metadata = metadata
        return cube

    def load_ml_model(self, id: Union[str, BatchJob]) -> "MlModel":
        """
        Loads a machine learning model from a STAC Item.

        :param id: STAC item reference, as URL, batch job (id) or user-uploaded file
        :return:

        .. versionadded:: 0.10.0
        """
        return MlModel.load_ml_model(connection=self, id=id)

    def create_service(self, graph: dict, type: str, **kwargs) -> Service:
        # TODO: type hint for graph: is it a nested or a flat one?
        req = self._build_request_with_process_graph(process_graph=graph, type=type, **kwargs)
        response = self.post(path="/services", json=req, expected_status=201)
        service_id = response.headers.get("OpenEO-Identifier")
        return Service(service_id, self)

    @deprecated("Use :py:meth:`openeo.rest.service.Service.delete_service` instead.", version="0.8.0")
    def remove_service(self, service_id: str):
        """
        Stop and remove a secondary web service.

        :param service_id: service identifier
        :return:
        """
        Service(service_id, self).delete_service()

    @deprecated("Use :py:meth:`openeo.rest.job.BatchJob.get_results` instead.", version="0.4.10")
    def job_results(self, job_id) -> dict:
        """Get batch job results metadata."""
        return BatchJob(job_id=job_id, connection=self).list_results()

    @deprecated("Use :py:meth:`openeo.rest.job.BatchJob.logs` instead.", version="0.4.10")
    def job_logs(self, job_id, offset) -> list:
        """Get batch job logs."""
        return BatchJob(job_id=job_id, connection=self).logs(offset=offset)

    def list_files(self):
        """
        Lists all files that the logged in user uploaded.

        :return: file_list: List of the user uploaded files.
        """

        files = self.get('/files', expected_status=200).json()['files']
        return VisualList("data-table", data=files, parameters={'columns': 'files'})

    def create_file(self, path):
        """
        Creates virtual file

        :return: file object.
        """
        # No endpoint just returns a file object.
        raise NotImplementedError()

    def _build_request_with_process_graph(self, process_graph: Union[dict, Any], **kwargs) -> dict:
        """
        Prepare a json payload with a process graph to submit to /result, /services, /jobs, ...
        :param process_graph: flat dict representing a process graph
        """
        result = kwargs
        process_graph = as_flat_graph(process_graph)
        if self._api_version.at_least("1.0.0"):
            if "process_graph" not in process_graph:
                process_graph = {"process_graph": process_graph}
            result["process"] = process_graph
        else:
            result["process_graph"] = process_graph
        return result

    # TODO: unify `download` and `execute` better: e.g. `download` always writes to disk, `execute` returns result (raw or as JSON decoded dict)
    def download(
            self,
            graph: Union[dict, str, Path],
            outputfile: Union[Path, str, None] = None,
            timeout: int = 30 * 60,
    ):
        """
        Downloads the result of a process graph synchronously,
        and save the result to the given file or return bytes object if no outputfile is specified.
        This method is useful to export binary content such as images. For json content, the execute method is recommended.

        :param graph: (flat) dict representing a process graph, or process graph as raw JSON string,
            or as local file path or URL
        :param outputfile: output file
        :param timeout: timeout to wait for response
        """
        request = self._build_request_with_process_graph(process_graph=graph)
        response = self.post(path="/result", json=request, expected_status=200, stream=True, timeout=timeout)

        if outputfile is not None:
            with Path(outputfile).open(mode="wb") as f:
                for chunk in response.iter_content(chunk_size=None):
                    f.write(chunk)
        else:
            return response.content

    def execute(self, process_graph: Union[dict, str, Path]):
        """
        Execute a process graph synchronously and return the result (assumed to be JSON).

        :param process_graph: (flat) dict representing a process graph, or process graph as raw JSON string,
            or as local file path or URL
        :return: parsed JSON response
        """
        req = self._build_request_with_process_graph(process_graph=process_graph)
        return self.post(path="/result", json=req, expected_status=200).json()

    def create_job(
            self, process_graph: Union[dict, str, Path],
            title: Optional[str] = None, description: Optional[str] = None,
            plan: Optional[str] = None, budget: Optional[float] = None,
            additional: Optional[dict] = None
    ) -> BatchJob:
        """
        Posts a job to the back end.

        :param process_graph: (flat) dict representing a process graph, or process graph as raw JSON string,
            or as local file path or URL
        :param title: String title of the job
        :param description: String description of the job
        :param plan: billing plan
        :param budget: maximum cost the request is allowed to produce
        :param additional: additional job options to pass to the backend
        :return: job_id: String Job id of the new created job
        """
        # TODO move all this (BatchJob factory) logic to BatchJob?
        req = self._build_request_with_process_graph(
            process_graph=process_graph,
            **dict_no_none(title=title, description=description, plan=plan, budget=budget)
        )
        if additional:
            # TODO: get rid of this non-standard field? https://github.com/Open-EO/openeo-api/issues/276
            req["job_options"] = additional

        response = self.post("/jobs", json=req, expected_status=201)

        job_id = None
        if "openeo-identifier" in response.headers:
            job_id = response.headers['openeo-identifier'].strip()
        elif "location" in response.headers:
            _log.warning("Backend did not explicitly respond with job id, will guess it from redirect URL.")
            job_id = response.headers['location'].split("/")[-1]
        if not job_id:
            raise OpenEoClientException("Job creation response did not contain a valid job id")
        return BatchJob(job_id=job_id, connection=self)

    def job(self, job_id: str) -> BatchJob:
        """
        Get the job based on the id. The job with the given id should already exist.

        Use :py:meth:`openeo.rest.connection.Connection.create_job` to create new jobs

        :param job_id: the job id of an existing job
        :return: A job object.
        """
        return BatchJob(job_id=job_id, connection=self)

    def service(self, service_id: str) -> Service:
        """
        Get the secondary web service based on the id. The service with the given id should already exist.

        Use :py:meth:`openeo.rest.connection.Connection.create_service` to create new services

        :param job_id: the service id of an existing secondary web service
        :return: A service object.
        """
        return Service(service_id, connection=self)

    def load_disk_collection(self, format: str, glob_pattern: str, options: dict = {}) -> ImageCollectionClient:
        """
        Loads image data from disk as an ImageCollection.

        :param format: the file format, e.g. 'GTiff'
        :param glob_pattern: a glob pattern that matches the files to load from disk
        :param options: options specific to the file format
        :return: the data as an ImageCollection
        """

        if self._api_version.at_least("1.0.0"):
            return DataCube.load_disk_collection(self, format, glob_pattern, **options)
        else:
            return ImageCollectionClient.load_disk_collection(self, format, glob_pattern, **options)

    def as_curl(self, data: Union[dict, DataCube], path="/result", method="POST") -> str:
        """
        Build curl command to evaluate given process graph or data cube
        (including authorization and content-type headers).

        :param data: process graph dictionary or :py:class:`~openeo.rest.datacube.DataCube` object
        :param path: endpoint to send request to
        :param method: HTTP method to use
        :return: curl command as a string
        """
        cmd = ["curl", "-i", "-X", method]
        cmd += ["-H", "Content-Type: application/json"]
        if isinstance(self.auth, BearerAuth):
            cmd += ["-H", f"Authorization: Bearer {self.auth.bearer}"]
        post_data = self._build_request_with_process_graph(data)
        post_json = json.dumps(post_data, separators=(',', ':'))
        cmd += ["--data", post_json]
        cmd += [self.build_url(path)]
        return " ".join(shlex.quote(c) for c in cmd)

    def version_info(self):
        """List version of the openEO client, API, back-end, etc."""
        capabilities = self.capabilities()
        return {
            "client": openeo.client_version(),
            "api": capabilities.api_version(),
            "backend": dict_no_none({
                "root_url": self.root_url,
                "version": capabilities.get("backend_version"),
                "processing:software": capabilities.get("processing:software"),
            }),
        }


def connect(
        url: Optional[str] = None,
        auth_type: Optional[str] = None, auth_options: Optional[dict] = None,
        session: Optional[requests.Session] = None,
        default_timeout: Optional[int] = None,
) -> Connection:
    """
    This method is the entry point to OpenEO.
    You typically create one connection object in your script or application
    and re-use it for all calls to that backend.

    If the backend requires authentication, you can pass authentication data directly to this function
    but it could be easier to authenticate as follows:

        >>> # For basic authentication
        >>> conn = connect(url).authenticate_basic(username="john", password="foo")
        >>> # For OpenID Connect authentication
        >>> conn = connect(url).authenticate_oidc(client_id="myclient")

    :param url: The http url of the OpenEO back-end.
    :param auth_type: Which authentication to use: None, "basic" or "oidc" (for OpenID Connect)
    :param auth_options: Options/arguments specific to the authentication type
    :param default_timeout: default timeout (in seconds) for requests
    :rtype: openeo.connections.Connection
    """

    def _config_log(message):
        _log.info(message)
        config_log(message)

    if url is None:
        default_backend = get_config_option("connection.default_backend")
        if default_backend:
            url = default_backend
            _config_log(f"Using default back-end URL {url!r} (from config)")
            default_backend_auto_auth = get_config_option("connection.default_backend.auto_authenticate")
            if default_backend_auto_auth and default_backend_auto_auth.lower() in {"basic", "oidc"}:
                auth_type = default_backend_auto_auth.lower()
                _config_log(f"Doing auto-authentication {auth_type!r} (from config)")

    if auth_type is None:
        auto_authenticate = get_config_option("connection.auto_authenticate")
        if auto_authenticate and auto_authenticate.lower() in {"basic", "oidc"}:
            auth_type = auto_authenticate.lower()
            _config_log(f"Doing auto-authentication {auth_type!r} (from config)")

    if not url:
        raise OpenEoClientException("No openEO back-end URL given or known to connect to.")
    connection = Connection(url, session=session, default_timeout=default_timeout)

    auth_type = auth_type.lower() if isinstance(auth_type, str) else auth_type
    if auth_type in {None, False, 'null', 'none'}:
        pass
    elif auth_type == "basic":
        connection.authenticate_basic(**(auth_options or {}))
    elif auth_type in {"oidc", "openid"}:
        connection.authenticate_oidc(**(auth_options or {}))
    else:
        raise ValueError("Unknown auth type {a!r}".format(a=auth_type))
    return connection


@deprecated("Use :py:func:`openeo.connect` instead", version="0.0.9")
def session(userid=None, endpoint: str = "https://openeo.org/openeo") -> Connection:
    """
    This method is the entry point to OpenEO. You typically create one session object in your script or application, per back-end.
    and re-use it for all calls to that backend.
    If the backend requires authentication, you should set pass your credentials.

    :param endpoint: The http url of an OpenEO endpoint.
    :rtype: openeo.sessions.Session
    """
    return connect(url=endpoint)


def paginate(con: Connection, url: str, params: dict = None, callback: Callable = lambda resp, page: resp):
    # TODO: make this a method `get_paginated` on `RestApiConnection`?
    # TODO: is it necessary to have `callback`? It's only used just before yielding,
    #       so it's probably cleaner (even for the caller) to to move it outside.
    page = 1
    while True:
        response = con.get(url, params=params).json()
        yield callback(response, page)
        next_links = [link for link in response.get("links", []) if link.get("rel") == "next" and "href" in link]
        if not next_links:
            break
        url = next_links[0]["href"]
        page += 1
        params = {}

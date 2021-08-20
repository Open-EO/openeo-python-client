"""
This module provides a Connection object to manage and persist settings when interacting with the OpenEO API.
"""
import datetime
import logging
import sys
import warnings
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Tuple, Union, Callable, Optional, Any, Iterator
from urllib.parse import urljoin

import requests
from deprecated.sphinx import deprecated
from requests import Response
from requests.auth import HTTPBasicAuth, AuthBase

import openeo
from openeo.capabilities import ApiVersionException, ComparableVersion
from openeo.internal.graph_building import PGNode, as_flat_graph
from openeo.internal.jupyter import VisualDict, VisualList
from openeo.internal.processes.builder import ProcessBuilderBase
from openeo.metadata import CollectionMetadata
from openeo.rest import OpenEoClientException, OpenEoApiError
from openeo.rest.auth.auth import NullAuth, BearerAuth
from openeo.rest.auth.config import RefreshTokenStore, AuthConfig
from openeo.rest.auth.oidc import OidcClientCredentialsAuthenticator, OidcAuthCodePkceAuthenticator, \
    OidcClientInfo, OidcAuthenticator, OidcRefreshTokenAuthenticator, OidcResourceOwnerPasswordAuthenticator, \
    OidcDeviceAuthenticator, OidcProviderInfo, OidcException, DefaultOidcClientGrant
from openeo.rest.datacube import DataCube
from openeo.rest.imagecollectionclient import ImageCollectionClient
from openeo.rest.job import RESTJob
from openeo.rest.rest_capabilities import RESTCapabilities
from openeo.rest.service import Service
from openeo.rest.udp import RESTUserDefinedProcess, Parameter
from openeo.util import ensure_list, legacy_alias, dict_no_none, rfc3339, load_json_resource

_log = logging.getLogger(__name__)


def url_join(root_url: str, path: str):
    """Join a base url and sub path properly."""
    return urljoin(root_url.rstrip('/') + '/', path.lstrip('/'))


class RestApiConnection:
    """Base connection class implementing generic REST API request functionality"""

    def __init__(self, root_url: str, auth: AuthBase = None, session: requests.Session = None,
                 default_timeout: int = None):
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
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug("Request `{m} {u}` with headers {h}, auth {a}, kwargs {k}".format(
                m=method.upper(), u=url, h=headers and headers.keys(), a=type(auth).__name__, k=list(kwargs.keys()))
            )
        resp = self.session.request(
            method=method,
            url=url,
            headers=self._merged_headers(headers),
            auth=auth,
            timeout=kwargs.pop("timeout", self.default_timeout),
            **kwargs
        )
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug("Got {r} headers {h!r}".format(r=resp, h=resp.headers))
        # Check for API errors and unexpected HTTP status codes as desired.
        status = resp.status_code
        expected_status = ensure_list(expected_status) if expected_status else []
        if check_error and status >= 400 and status not in expected_status:
            self._raise_api_error(resp)
        if expected_status and status not in expected_status:
            raise OpenEoClientException("Got status code {s!r} for `{m} {p}` (expected {e!r})".format(
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

    # Temporary workaround flag to enable for backends (e.g. EURAC) that expect id_token to be sent as bearer token
    # TODO DEPRECATED To remove when all backends properly expect access_token
    # see https://github.com/Open-EO/openeo-wcps-driver/issues/45
    oidc_auth_user_id_token_as_bearer = False

    def __init__(
            self, url: str, auth: AuthBase = None, session: requests.Session = None, default_timeout: int = None,
            auth_config: AuthConfig = None, refresh_token_store: RefreshTokenStore = None
    ):
        """
        Constructor of Connection, authenticates user.

        :param url: String Backend root url
        """
        if "://" not in url:
            url = "https://" + url
        self._orig_url = url
        super().__init__(
            root_url=self.version_discovery(url, session=session),
            auth=auth, session=session, default_timeout=default_timeout
        )
        self._capabilities_cache = {}

        # Initial API version check.
        if self._api_version.below(self._MINIMUM_API_VERSION):
            raise ApiVersionException("OpenEO API version should be at least {m!s}, but got {v!s}".format(
                m=self._MINIMUM_API_VERSION, v=self._api_version)
            )

        self._auth_config = auth_config
        self._refresh_token_store = refresh_token_store

    @classmethod
    def version_discovery(cls, url: str, session: requests.Session = None) -> str:
        """
        Do automatic openEO API version discovery from given url, using a "well-known URI" strategy.

        :param url: initial backend url (not including "/.well-known/openeo")
        :return: root url of highest supported backend version
        """
        try:
            well_known_url_response = RestApiConnection(url, session=session).get("/.well-known/openeo")
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
            self.auth = BearerAuth(bearer='basic//{t}'.format(t=resp["access_token"]))
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
            default_client_grant_types: Union[None, List[DefaultOidcClientGrant]] = None
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
        if client_id is None and default_client_grant_types:
            # Try "default_client" from backend's provider info.
            _log.debug("No client_id given: checking default client in backend's provider info")
            client_id = provider.get_default_client_id(grant_types=default_client_grant_types)
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
            provider_id: str,
            store_refresh_token: bool = False
    ) -> 'Connection':
        """
        Authenticate through OIDC and set up bearer token (based on OIDC access_token) for further requests.
        """
        tokens = authenticator.get_tokens(request_refresh_token=store_refresh_token)
        _log.info("Obtained tokens: {t}".format(t=[k for k, v in tokens._asdict().items() if v]))
        if store_refresh_token:
            if tokens.refresh_token:
                self._get_refresh_token_store().set_refresh_token(
                    issuer=authenticator.provider_info.issuer,
                    client_id=authenticator.client_id,
                    refresh_token=tokens.refresh_token
                )
            else:
                _log.warning("OIDC token response did not contain refresh token.")
        token = tokens.access_token if not self.oidc_auth_user_id_token_as_bearer else tokens.id_token
        if self._api_version.at_least("1.0.0"):
            self.auth = BearerAuth(bearer='oidc/{p}/{t}'.format(p=provider_id, t=token))
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
            default_client_grant_types=[DefaultOidcClientGrant.AUTH_CODE_PKCE],
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
            self, client_id: str = None, refresh_token: str = None, client_secret: str = None, provider_id: str = None
    ) -> 'Connection':
        """
        OpenId Connect Refresh Token
        """
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret,
            default_client_grant_types=[DefaultOidcClientGrant.REFRESH_TOKEN],
        )

        if refresh_token is None:
            refresh_token = self._get_refresh_token_store().get_refresh_token(
                issuer=client_info.provider.issuer,
                client_id=client_info.client_id
            )
            if refresh_token is None:
                raise OpenEoClientException("No refresh token given or found")

        authenticator = OidcRefreshTokenAuthenticator(client_info=client_info, refresh_token=refresh_token)
        return self._authenticate_oidc(authenticator, provider_id=provider_id)

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
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret,
            default_client_grant_types=[DefaultOidcClientGrant.DEVICE_CODE_PKCE],
        )
        authenticator = OidcDeviceAuthenticator(client_info=client_info, use_pkce=use_pkce, **kwargs)
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc(
            self,
            provider_id: str = None,
            client_id: Union[str, None] = None, client_secret: Union[str, None] = None,
            store_refresh_token: bool = True
    ):
        """
        Do OpenID Connect authentication, first trying refresh tokens and falling back on device code flow.

        .. versionadded:: 0.6.0
        """
        provider_id, client_info = self._get_oidc_provider_and_client_info(
            provider_id=provider_id, client_id=client_id, client_secret=client_secret,
            default_client_grant_types=[DefaultOidcClientGrant.DEVICE_CODE_PKCE, DefaultOidcClientGrant.REFRESH_TOKEN]
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
                    authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token
                )
                # TODO: pluggable/jupyter-aware display function?
                print("Authenticated using refresh token.")
                return con
            except OidcException as e:
                _log.info("Refresh token based authentication failed: {e}.".format(e=e))

        # Fall back on device code flow
        # TODO: make it possible to do other fallback flows too?
        _log.info("Trying device code flow.")
        authenticator = OidcDeviceAuthenticator(client_info=client_info)
        con = self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)
        print("Authenticated using device code flow.")
        return con

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
        Loads all available imagecollections types.

        :return: list of collection meta data dictionaries
        """
        data  = self.get('/collections').json()["collections"]
        return VisualList("collections", data = data)

    def list_collection_ids(self) -> List[str]:
        """
        Get list of all collection ids

        :return: list of collection ids
        """
        return [collection['id'] for collection in self.list_collections() if 'id' in collection]

    def capabilities(self) -> RESTCapabilities:
        """
        Loads all available capabilities.

        :return: data_dict: Dict All available data types
        """
        if "capabilities" not in self._capabilities_cache:
            self._capabilities_cache["capabilities"] = RESTCapabilities(
                self.get('/', expected_status=200).json(),
                self._orig_url
            )
        return self._capabilities_cache["capabilities"]



    def list_output_formats(self) -> dict:
        if self._api_version.at_least("1.0.0"):
            return self.list_file_formats()["output"]
        else:
            return self.get('/output_formats').json()

    list_file_types = legacy_alias(list_output_formats, "list_file_types")

    def list_file_formats(self) -> dict:
        """
        Get available input and output formats
        """
        if "file_formats" not in self._capabilities_cache:
            self._capabilities_cache["file_formats"] = self.get('/file_formats').json()
        return VisualDict("file-formats", data = self._capabilities_cache["file_formats"])

    def list_service_types(self) -> dict:
        """
        Loads all available service types.

        :return: data_dict: Dict All available service types
        """
        if "service_types" not in self._capabilities_cache:
            self._capabilities_cache["service_types"] = self.get('/service_types').json()
        return VisualDict("service-types", data = self._capabilities_cache["service_types"])

    def list_udf_runtimes(self) -> dict:
        """
        Loads all available UDF runtimes.

        :return: data_dict: Dict All available UDF runtimes
        """
        if "udf_runtimes" not in self._capabilities_cache:
            self._capabilities_cache["udf_runtimes"] = self.get('/udf_runtimes').json()
        return VisualDict("udf-runtimes", data = self._capabilities_cache["udf_runtimes"])

    def list_services(self) -> dict:
        """
        Loads all available services of the authenticated user.

        :return: data_dict: Dict All available services
        """
        # TODO return parsed service objects
        services = self.get('/services').json()["services"]
        return VisualList("data-table", data = services, parameters = {'columns': 'services'})

    def describe_collection(self, name) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.

        :param name: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """
        data = self.get('/collections/{}'.format(name)).json()
        return VisualDict("collection", data = data)

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
        return CollectionMetadata(metadata=self.describe_collection(name))

    def list_processes(self, namespace:str=None) -> List[dict]:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        """
        Loads all available processes of the back end.

        :param namespace: The namespace for which to list processes.

        :return: processes_dict: Dict All available processes of the back end.
        """
        namespace = "/" + namespace if namespace is not None else ""
        data = self.get('/processes' + namespace).json()["processes"]
        return VisualList("processes", data = data, parameters = {'show-graph': True, 'provide-download': False})

    def list_jobs(self) -> List[dict]:
        """
        Lists all jobs of the authenticated user.

        :return: job_list: Dict of all jobs of the user.
        """
        # TODO: Parse the result so that there get Job classes returned?
        jobs = self.get('/jobs').json()["jobs"]
        return VisualList("data-table", data = jobs, parameters = {'columns': 'jobs'})

    def save_user_defined_process(
            self, user_defined_process_id: str,
            process_graph: Union[dict, ProcessBuilderBase],
            parameters: List[Union[dict, Parameter]] = None,
            public: bool = False, summary: str = None, description: str = None
    ) -> RESTUserDefinedProcess:
        """
        Saves a process graph and its metadata in the backend as a user-defined process for the authenticated user.

        :param user_defined_process_id: unique identifier for the user-defined process
        :param process_graph: a process graph
        :param parameters: a list of parameters
        :param public: visible to other users?
        :param summary: A short summary of what the process does.
        :param description: Detailed description to explain the entity. CommonMark 0.29 syntax MAY be used for rich text representation.
        :return: a RESTUserDefinedProcess instance
        """
        if user_defined_process_id in set(p["id"] for p in self.list_processes()):
            warnings.warn("Defining user-defined process {u!r} with same id as a pre-defined process".format(
                u=user_defined_process_id))
        if not parameters:
            warnings.warn("Defining user-defined process {u!r} without parameters".format(u=user_defined_process_id))
        udp = RESTUserDefinedProcess(user_defined_process_id=user_defined_process_id, connection=self)
        udp.store(process_graph=process_graph, parameters=parameters, public=public,summary=summary,description=description)
        return udp

    def list_user_defined_processes(self) -> List[dict]:
        """
        Lists all user-defined processes of the authenticated user.
        """
        data = self.get("/process_graphs").json()["processes"]
        return VisualList("processes", data=data, parameters = {'show-graph': True, 'provide-download': False})

    def user_defined_process(self, user_defined_process_id: str) -> RESTUserDefinedProcess:
        """
        Get the user-defined process based on its id. The process with the given id should already exist.

        :param user_defined_process_id: the id of the user-defined process
        :return: a RESTUserDefinedProcess instance
        """
        return RESTUserDefinedProcess(user_defined_process_id=user_defined_process_id, connection=self)

    def validate_processgraph(self, process_graph):
        # Endpoint: POST /validate
        raise NotImplementedError()

    @property
    def _api_version(self) -> ComparableVersion:
        # TODO make this a public property (it's also useful outside the Connection class)
        return self.capabilities().api_version_check

    def datacube_from_process(self, process_id: str, namespace: str = None, **kwargs) -> DataCube:
        """
        Load a raster datacube, from a custom process.

        :param process_id: The process id of the custom process.
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
        Construct a :py:class:`DataCube` from a flat dictionaty representation of a process graph.

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
            properties: Optional[Dict[str, Union[str, PGNode, Callable]]] = None
    ) -> DataCube:
        """
        Load a DataCube by collection id.

        :param collection_id: image collection identifier
        :param spatial_extent: limit data to specified bounding box or polygons
        :param temporal_extent: limit data to specified temporal interval
        :param bands: only add the specified bands
        :param properties: limit data by metadata property predicates
        :return: a datacube containing the requested data
        """
        if self._api_version.at_least("1.0.0"):
            return DataCube.load_collection(
                collection_id=collection_id, connection=self,
                spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=bands, properties=properties
            )
        else:
            return ImageCollectionClient.load_collection(
                collection_id=collection_id, session=self,
                spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=bands
            )

    imagecollection = legacy_alias(load_collection, name="imagecollection")

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

    @deprecated("Use :py:meth:`openeo.rest.job.RESTJob.get_results` instead.", version="0.4.10")
    def job_results(self, job_id) -> dict:
        """Get batch job results metadata."""
        return RESTJob(job_id, connection=self).list_results()

    @deprecated("Use :py:meth:`openeo.rest.job.RESTJob.logs` instead.", version="0.4.10")
    def job_logs(self, job_id, offset) -> list:
        """Get batch job logs."""
        return RESTJob(job_id, connection=self).logs(offset=offset)

    def list_files(self):
        """
        Lists all files that the logged in user uploaded.

        :return: file_list: List of the user uploaded files.
        """

        files = self.get('/files').json()['files']
        return VisualList("data-table", data = files, parameters = {'columns': 'files'})

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
            result["process"] = {"process_graph": process_graph}
        else:
            result["process_graph"] = process_graph
        return result

    # TODO: unify `download` and `execute` better: e.g. `download` always writes to disk, `execute` returns result (raw or as JSON decoded dict)
    def download(self, graph: dict, outputfile: Union[Path, str, None] = None, timeout:int=30*60):
        """
        Downloads the result of a process graph synchronously,
        and save the result to the given file or return bytes object if no outputfile is specified.
        This method is useful to export binary content such as images. For json content, the execute method is recommended.

        :param graph: (flat) dict representing a process graph
        :param outputfile: output file
        """
        request = self._build_request_with_process_graph(process_graph=graph)
        response = self.post(path="/result", json=request, expected_status=200, stream=True, timeout=timeout)

        if outputfile is not None:
            with Path(outputfile).open(mode="wb") as f:
                for chunk in response.iter_content(chunk_size=None):
                    f.write(chunk)
        else:
            return response.content

    def execute(self, process_graph: dict):
        """
        Execute a process graph synchronously.

        :param process_graph: (flat) dict representing a process graph
        """
        req = self._build_request_with_process_graph(process_graph=process_graph)
        return self.post(path="/result", json=req, expected_status=200).json()

    def create_job(self, process_graph: dict, title: str = None, description: str = None,
                   plan: str = None, budget=None,
                   additional: Dict = None) -> RESTJob:
        """
        Posts a job to the back end.

        :param process_graph: (flat) dict representing process graph
        :param title: String title of the job
        :param description: String description of the job
        :param plan: billing plan
        :param budget: Budget
        :param additional: additional job options to pass to the backend
        :return: job_id: String Job id of the new created job
        """
        # TODO move all this (RESTJob factory) logic to RESTJob?
        req = self._build_request_with_process_graph(
            process_graph=process_graph,
            **dict_no_none(title=title, description=description, plan=plan, budget=budget)
        )
        if additional:
            # TODO: get rid of this non-standard field? https://github.com/Open-EO/openeo-api/issues/276
            req["job_options"] = additional

        response = self.post("/jobs", json=req, expected_status=201)

        if "openeo-identifier" in response.headers:
            job_id = response.headers['openeo-identifier']
        elif "location" in response.headers:
            _log.warning("Backend did not explicitly respond with job id, will guess it from redirect URL.")
            job_id = response.headers['location'].split("/")[-1]
        else:
            raise OpenEoClientException("Failed fo extract job id")
        return RESTJob(job_id, self)

    def job(self, job_id: str):
        """
        Get the job based on the id. The job with the given id should already exist.
        
        Use :py:meth:`openeo.rest.connection.Connection.create_job` to create new jobs

        :param job_id: the job id of an existing job
        :return: A job object.
        """
        return RESTJob(job_id, self)

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


def connect(url, auth_type: str = None, auth_options: dict = {}, session: requests.Session = None,
            default_timeout: int = None) -> Connection:
    """
    This method is the entry point to OpenEO.
    You typically create one connection object in your script or application
    and re-use it for all calls to that backend.

    If the backend requires authentication, you should can pass authentication data directly to this function
    but it could be easier to authenticate as follows:

        >>> # For basic authentication
        >>> conn = connect(url).authenticate_basic(username="john", password="foo")
        >>> # For OpenID Connect authentication
        >>> conn = connect(url).authenticate_oidc(client_id="myclient")

    :param url: The http url of an OpenEO endpoint.
    :param auth_type: Which authentication to use: None, "basic" or "oidc" (for OpenID Connect)
    :param auth_options: Options/arguments specific to the authentication type
    :param default_timeout: default timeout (in seconds) for requests
    :rtype: openeo.connections.Connection
    """
    connection = Connection(url, session=session, default_timeout=default_timeout)
    auth_type = auth_type.lower() if isinstance(auth_type, str) else auth_type
    if auth_type in {None, 'null', 'none'}:
        pass
    elif auth_type == "basic":
        connection.authenticate_basic(**auth_options)
    elif auth_type in {"oidc", "openid"}:
        connection.authenticate_oidc(**auth_options)
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


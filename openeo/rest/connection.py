"""
This module provides a Connection object to manage and persist settings when interacting with the OpenEO API.
"""

import logging
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Union, Callable
from urllib.parse import urljoin

import requests
from deprecated import deprecated
from requests import Response
from requests.auth import HTTPBasicAuth, AuthBase

import openeo
from openeo.capabilities import ApiVersionException, ComparableVersion
from openeo.imagecollection import CollectionMetadata
from openeo.internal.graph_building import PGNode
from openeo.rest import OpenEoClientException
from openeo.rest.auth.auth import NullAuth, BearerAuth
from openeo.rest.auth.config import RefreshTokenStore, AuthConfig
from openeo.rest.auth.oidc import OidcClientCredentialsAuthenticator, OidcAuthCodePkceAuthenticator, \
    OidcClientInfo, OidcAuthenticator, OidcRefreshTokenAuthenticator, OidcResourceOwnerPasswordAuthenticator, \
    OidcDeviceAuthenticator, OidcProviderInfo
from openeo.rest.datacube import DataCube
from openeo.rest.imagecollectionclient import ImageCollectionClient
from openeo.rest.job import RESTJob
from openeo.rest.rest_capabilities import RESTCapabilities
from openeo.rest.udp import RESTUserDefinedProcess, Parameter
from openeo.util import ensure_list

_log = logging.getLogger(__name__)


def url_join(root_url: str, path: str):
    """Join a base url and sub path properly."""
    return urljoin(root_url.rstrip('/') + '/', path.lstrip('/'))


class OpenEoApiError(OpenEoClientException):
    """
    Error returned by OpenEO API according to https://open-eo.github.io/openeo-api/errors/
    """

    def __init__(self, http_status_code: int = None,
                 code: str = 'unknown', message: str = 'unknown error', id: str = None, url: str = None):
        self.http_status_code = http_status_code
        self.code = code
        self.message = message
        self.id = id
        self.url = url
        super().__init__("[{s}] {c}: {m}".format(s=self.http_status_code, c=self.code, m=self.message))


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

    def request(self, method: str, path: str, headers: dict = None, auth: AuthBase = None,
                check_error=True, expected_status=None, **kwargs):
        """Generic request send"""
        url = self.build_url(path)
        auth = auth or self.auth
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
        return self.request("post", path=path, json=json, **kwargs)

    def delete(self, path, **kwargs) -> Response:
        """
        Do DELETE request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("delete", path=path, **kwargs)

    def patch(self, path) -> Response:
        """
        Do PATCH request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("patch", path=path)

    def put(self, path, headers: dict = None, data=None, **kwargs) -> Response:
        """
        Do PUT request to REST API.

        :param path: API path (without root url)
        :param headers: headers that gets added to the request.
        :param data: data that gets added to the request.
        :return: response: Response
        """
        return self.request("put", path=path, data=data, headers=headers, **kwargs)

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
            self, url, auth: AuthBase = None, session: requests.Session = None, default_timeout: int = None,
            auth_config: AuthConfig = None, refresh_token_store: RefreshTokenStore = None
    ):
        """
        Constructor of Connection, authenticates user.

        :param url: String Backend root url
        """
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

        self._auth_config = auth_config or AuthConfig()
        self._refresh_token_store = refresh_token_store or RefreshTokenStore()

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

    def authenticate_basic(self, username: str = None, password: str = None) -> 'Connection':
        """
        Authenticate a user to the backend using basic username and password.

        :param username: User name
        :param password: User passphrase
        """
        if username is None:
            username, password = self._auth_config.get_basic_auth(backend=self._orig_url)
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

    def authenticate_OIDC(
            self, client_id: str,
            provider_id: str = None,
            webbrowser_open=None,
            timeout=120,
            server_address: Tuple[str, int] = None
    ) -> 'Connection':
        """
        Authenticates a user to the backend using OpenID Connect.

        :param client_id: Client id to use for OpenID Connect authentication
        :param webbrowser_open: optional handler for the initial OAuth authentication request
            (opens a webbrowser by default)
        :param timeout: number of seconds after which to abort the authentication procedure
        :param server_address: optional tuple (hostname, port_number) to serve the OAuth redirect callback on

        TODO: deprecated?
        """
        # TODO: option to increase log level temporarily?
        provider_id, provider = self._get_oidc_provider(provider_id)

        client_info = OidcClientInfo(client_id=client_id, provider=provider)
        authenticator = OidcAuthCodePkceAuthenticator(
            client_info=client_info,
            webbrowser_open=webbrowser_open,
            timeout=timeout,
            server_address=server_address,
        )
        return self._authenticate_oidc(authenticator, provider_id=provider_id)

    def _get_oidc_provider(self, provider_id: Union[str, None] = None) -> Tuple[str, OidcProviderInfo]:
        """
        Get OpenID Connect discovery URL for given provider_id

        :param provider_id: id of OIDC provider as specified by backend (/credentials/oidc).
            Can be None if there is just one provider.
        :return: updated provider_id and provider info object
        """
        if self._api_version.at_least("1.0.0"):
            oidc_info = self.get("/credentials/oidc", expected_status=200).json()
            providers = {p["id"]: p for p in oidc_info["providers"]}
            _log.info("Found OIDC providers: {p}".format(p=list(providers.keys())))
            if provider_id:
                if provider_id not in providers:
                    raise OpenEoClientException("Requested provider {r!r} not available. Should be one of {p}.".format(
                        r=provider_id, p=list(providers.keys()))
                    )
                provider = providers[provider_id]
            elif len(providers) == 1:
                # No provider id given, but there is only one anyway: we can handle that.
                provider_id, provider = providers.popitem()
            else:
                raise OpenEoClientException("No provider_id given. Available: {p!r}.".format(
                    p=list(providers.keys()))
                )
            provider = OidcProviderInfo(issuer=provider["issuer"], scopes=provider.get("scopes"))
        else:
            # Per spec: '/credentials/oidc' will redirect to  OpenID Connect discovery document
            provider = OidcProviderInfo(discovery_url=self.build_url('/credentials/oidc'))
        return provider_id, provider

    def _authenticate_oidc(
            self,
            authenticator: OidcAuthenticator,
            provider_id: str,
            store_refresh_token: bool = False
    ) -> 'Connection':
        """
        Authenticate through OIDC and set up bearer token (based on OIDC access_token) for further requests.
        """
        tokens = authenticator.get_tokens()
        _log.info("Obtained tokens: {t}".format(t=[k for k, v in tokens._asdict().items() if v]))
        if tokens.refresh_token and store_refresh_token:
            self._refresh_token_store.set_refresh_token(
                issuer=authenticator.provider_info.issuer,
                client_id=authenticator.client_id,
                refresh_token=tokens.refresh_token
            )
        token = tokens.access_token if not self.oidc_auth_user_id_token_as_bearer else tokens.id_token
        if self._api_version.at_least("1.0.0"):
            self.auth = BearerAuth(bearer='oidc/{p}/{t}'.format(p=provider_id, t=token))
        else:
            self.auth = BearerAuth(bearer=token)
        return self

    def authenticate_oidc_authorization_code(
            self,
            client_id: str,
            client_secret: str = None,
            provider_id: str = None,
            timeout: int = None,
            server_address: Tuple[str, int] = None,
            webbrowser_open: Callable = None,
            store_refresh_token=False,
    ) -> 'Connection':
        """
        OpenID Connect Authorization Code Flow (with PKCE).

        WARNING: this API is in experimental phase
        """
        provider_id, provider = self._get_oidc_provider(provider_id)
        # TODO: load client info and settings from config file?
        client_info = OidcClientInfo(client_id=client_id, client_secret=client_secret, provider=provider)
        authenticator = OidcAuthCodePkceAuthenticator(
            client_info=client_info,
            webbrowser_open=webbrowser_open, timeout=timeout, server_address=server_address
        )
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc_client_credentials(
            self,
            client_id: str,
            client_secret: str = None,
            provider_id: str = None,
            store_refresh_token=False,
    ) -> 'Connection':
        """
        OpenID Connect Client Credentials flow.

        WARNING: this API is in experimental phase
        """
        provider_id, provider = self._get_oidc_provider(provider_id)
        # TODO: load credentials from file/config
        client_info = OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret)
        authenticator = OidcClientCredentialsAuthenticator(client_info=client_info)
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc_resource_owner_password_credentials(
            self, client_id: str, username: str, password: str, client_secret: str = None, provider_id: str = None,
            store_refresh_token=False
    ) -> 'Connection':
        """
        OpenId Connect Resource Owner Password Credentials

        WARNING: this API is in experimental phase
        """
        provider_id, provider = self._get_oidc_provider(provider_id)
        # TODO: load password from file/config
        client_info = OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret)
        authenticator = OidcResourceOwnerPasswordAuthenticator(
            client_info=client_info, username=username, password=password
        )
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def authenticate_oidc_refresh_token(
            self, client_id: str = None, refresh_token: str = None, client_secret: str = None, provider_id: str = None
    ) -> 'Connection':
        """
        OpenId Connect Refresh Token

        WARNING: this API is in experimental phase
        """
        provider_id, provider = self._get_oidc_provider(provider_id)
        if client_id is None:
            client_id, client_secret = self._auth_config.get_oidc_client_configs(
                backend=self._orig_url, provider_id=provider_id
            )
            if client_id is None:
                raise OpenEoClientException("No client ID given or found.")

        if refresh_token is None:
            refresh_token = self._refresh_token_store.get_refresh_token(issuer=provider.issuer, client_id=client_id)
            if refresh_token is None:
                raise OpenEoClientException("No refresh token given or found")

        client_info = OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret)
        authenticator = OidcRefreshTokenAuthenticator(client_info=client_info, refresh_token=refresh_token)
        return self._authenticate_oidc(authenticator, provider_id=provider_id)

    def authenticate_oidc_device(
            self, client_id: str, client_secret: str, provider_id: str = None,
            store_refresh_token=False,
            **kwargs
    ) -> 'Connection':
        """
        Authenticate with OAuth Device Authorization grant/flow

        WARNING: this API is in experimental phase
        """
        provider_id, provider = self._get_oidc_provider(provider_id)
        client_info = OidcClientInfo(client_id=client_id, provider=provider, client_secret=client_secret)
        authenticator = OidcDeviceAuthenticator(client_info=client_info, **kwargs)
        return self._authenticate_oidc(authenticator, provider_id=provider_id, store_refresh_token=store_refresh_token)

    def describe_account(self) -> str:
        """
        Describes the currently authenticated user account.
        """
        return self.get('/me').json()

    def user_jobs(self) -> dict:
        """
        Loads all jobs of the current user.

        :return: jobs: Dict All jobs of the user
        """
        # TODO duplication with `list_jobs()` method
        return self.get('/jobs').json()["jobs"]

    def list_collections(self) -> List[dict]:
        """
        Loads all available imagecollections types.

        :return: list of collection meta data dictionaries
        """
        return self.get('/collections').json()["collections"]

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
            self._capabilities_cache["capabilities"] = RESTCapabilities(self.get('/').json())
        return self._capabilities_cache["capabilities"]

    @deprecated("Use 'list_output_formats' instead")
    def list_file_types(self) -> dict:
        return self.list_output_formats()

    def list_output_formats(self) -> dict:
        if self._api_version.at_least("1.0.0"):
            return self.list_file_formats()["output"]
        else:
            return self.get('/output_formats').json()

    def list_file_formats(self) -> dict:
        """
        Get available input and output formats
        """
        if "file_formats" not in self._capabilities_cache:
            self._capabilities_cache["file_formats"] = self.get('/file_formats').json()
        return self._capabilities_cache["file_formats"]

    def list_service_types(self) -> dict:
        """
        Loads all available service types.

        :return: data_dict: Dict All available service types
        """
        return self.get('/service_types').json()

    def list_services(self) -> dict:
        """
        Loads all available services of the authenticated user.

        :return: data_dict: Dict All available service types
        """
        # TODO return parsed service objects
        return self.get('/services').json()

    def describe_collection(self, name) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.

        :param name: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """
        return self.get('/collections/{}'.format(name)).json()

    def collection_metadata(self, name) -> CollectionMetadata:
        return CollectionMetadata(metadata=self.describe_collection(name))

    def list_processes(self) -> dict:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        """
        Loads all available processes of the back end.

        :return: processes_dict: Dict All available processes of the back end.
        """
        return self.get('/processes').json()["processes"]

    def list_jobs(self) -> dict:
        """
        Lists all jobs of the authenticated user.

        :return: job_list: Dict of all jobs of the user.
        """
        # TODO: Maybe format the result so that there get Job classes returned.
        # TODO: duplication with `user_jobs()` method
        return self.get('/jobs').json()["jobs"]

    def save_user_defined_process(
            self, user_defined_process_id: str, process_graph: dict,
            parameters: List[Union[dict, Parameter]] = None, public: bool = False) -> RESTUserDefinedProcess:
        """
        Saves a process graph and its metadata in the backend as a user-defined process for the authenticated user.

        :param user_defined_process_id: unique identifier for the user-defined process
        :param process_graph: a process graph
        :param parameters: a list of parameters
        :param public: visible to other users?
        :return: a RESTUserDefinedProcess instance
        """
        udp = RESTUserDefinedProcess(user_defined_process_id=user_defined_process_id, connection=self)
        udp.store(process_graph=process_graph, parameters=parameters, public=public)
        return udp

    def list_user_defined_processes(self) -> List[dict]:
        """
        Lists all user-defined processes of the authenticated user.
        """
        return self.get("/process_graphs").json()["processes"]

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

    def list_processgraphs(self, process_graph):
        # Endpoint: GET /process_graphs
        raise NotImplementedError()

    @property
    def _api_version(self) -> ComparableVersion:
        # TODO make this a public property (it's also useful outside the Connection class)
        return self.capabilities().api_version_check

    def datacube_from_process(self,process_id:str, **kwargs) -> DataCube:
        """
        Load a raster datacube, from a custom process.

        @param process_id: The process id of the custom process.
        @param kwargs: The arguments of the custom process
        @return: A DataCube, without valid metadata, as the client is not aware of this custom process.
        """

        if self._api_version.at_least("1.0.0"):
            graph = PGNode(process_id,kwargs)
            return DataCube(graph,self)
        else:
            raise OpenEoClientException("This method requires support for at least version 1.0.0 in the openEO backend.")

    def load_collection(self, collection_id: str, **kwargs) -> Union[ImageCollectionClient, DataCube]:
        """
        Load an image collection by collection id

        see :py:meth:`openeo.rest.imagecollectionclient.ImageCollectionClient.load_collection`
        for available arguments.

        :param collection_id: image collection identifier (string)
        :return: ImageCollectionClient
        """
        if self._api_version.at_least("1.0.0"):
            return DataCube.load_collection(collection_id=collection_id, connection=self, **kwargs)
        else:
            return ImageCollectionClient.load_collection(collection_id=collection_id, session=self, **kwargs)

    # Legacy alias.
    imagecollection = load_collection

    def create_service(self, graph: dict, type: str, **kwargs) -> dict:
        # TODO: type hint for graph: is it a nested or a flat one?
        req = self._build_request_with_process_graph(process_graph=graph, type=type, **kwargs)
        response = self.post(path="/services", json=req, expected_status=201)
        # TODO: "location" is url of the service metadata, not (base) url of service (https://github.com/Open-EO/openeo-api/issues/269)
        # TODO: fetch this metadata and return a full metadata object instead?
        return {
            'url': response.headers.get('Location'),
            'service_id': response.headers.get("OpenEO-Identifier"),
        }

    def remove_service(self, service_id: str):
        """
        Stop and remove a secondary web service.

        :param service_id: service identifier
        :return:
        """
        response = self.delete('/services/' + service_id)

    def job_results(self, job_id):
        return self.get("/jobs/{}/results".format(job_id)).json()

    def job_logs(self, job_id, offset):
        return self.get("/jobs/{}/logs".format(job_id), params={'offset': offset}).json()

    def list_files(self):
        """
        Lists all files that the logged in user uploaded.

        :return: file_list: List of the user uploaded files.
        """

        return self.get('/files').json()['files']

    def create_file(self, path):
        """
        Creates virtual file

        :return: file object.
        """
        # No endpoint just returns a file object.
        raise NotImplementedError()

    def _build_request_with_process_graph(self, process_graph: dict, **kwargs) -> dict:
        """
        Prepare a json payload with a process graph to submit to /result, /services, /jobs, ...
        :param process_graph: flat dict representing a process graph
        """
        result = kwargs
        if self._api_version.at_least("1.0.0"):
            result["process"] = {"process_graph": process_graph}
        else:
            result["process_graph"] = process_graph
        return result

    # TODO: Maybe rename to execute and merge with execute().
    def download(self, graph: dict, outputfile):
        """
        Downloads the result of a process graph synchronously, and save the result to the given file.
        This method is useful to export binary content such as images. For json content, the execute method is recommended.

        :param graph: (flat) dict representing a process graph
        :param outputfile: output file
        """
        request = self._build_request_with_process_graph(process_graph=graph)
        r = self.post(path="/result", json=request, stream=True, timeout=1000)
        with Path(outputfile).open(mode="wb") as f:
            shutil.copyfileobj(r.raw, f)

    def execute(self, process_graph: dict):
        """
        Execute a process graph synchronously.

        :param process_graph: (flat) dict representing a process graph
        """
        req = self._build_request_with_process_graph(process_graph=process_graph)
        return self.post(path="/result", json=req).json()

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
            title=title, description=description, plan=plan, budget=budget
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
        >>> conn = connect(url).authenticate_OIDC(client_id="myclient")

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
        connection.authenticate_OIDC(**auth_options)
    else:
        raise ValueError("Unknown auth type {a!r}".format(a=auth_type))
    return connection


@deprecated("Use openeo.connect")
def session(userid=None, endpoint: str = "https://openeo.org/openeo") -> Connection:
    """
    Deprecated, use openeo.connect
    This method is the entry point to OpenEO. You typically create one session object in your script or application, per back-end.
    and re-use it for all calls to that backend.
    If the backend requires authentication, you should set pass your credentials.

    :param endpoint: The http url of an OpenEO endpoint.
    :rtype: openeo.sessions.Session
    """
    return connect(url=endpoint)

"""
This module provides a Connection object to manage and persist settings when interacting with the OpenEO API.
"""

import logging
import shutil
from typing import Dict, List
from urllib.parse import urljoin

import requests
from deprecated import deprecated
from requests import Response
from requests.auth import HTTPBasicAuth, AuthBase

import openeo
from openeo.capabilities import Capabilities
from openeo.imagecollection import CollectionMetadata
from openeo.rest.auth.auth import NullAuth, BearerAuth
from openeo.rest.imagecollectionclient import ImageCollectionClient
from openeo.rest.job import RESTJob
from openeo.rest.rest_capabilities import RESTCapabilities

_log = logging.getLogger(__name__)


def url_join(root_url: str, path: str):
    """Join a base url and sub path properly."""
    return urljoin(root_url.rstrip('/') + '/', path.lstrip('/'))


class OpenEoApiError(Exception):
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

    def __init__(self, root_url: str, auth: AuthBase = None, session: requests.Session = None):
        self._root_url = root_url
        self.session = session or requests.Session()
        self.auth = auth or NullAuth()
        self.default_headers = {
            "User-Agent": "openeo-python-client/{v}".format(v=openeo.client_version())
        }

    def build_url(self, path: str):
        return url_join(self._root_url, path)

    def _merged_headers(self, headers: dict) -> dict:
        """Merge default headers with given headers"""
        result = self.default_headers.copy()
        if headers:
            result.update(headers)
        return result

    def request(self, method: str, path: str, headers: dict = None, auth: AuthBase = None, check_status=True, **kwargs):
        """Generic request send"""
        resp = self.session.request(
            method=method,
            url=self.build_url(path),
            headers=self._merged_headers(headers),
            auth=auth or self.auth,
            **kwargs
        )
        if check_status:
            # TODO: option to specify the list/range of expected status codes?
            if resp.status_code >= 400:
                self._raise_api_error(resp)
        return resp

    def _raise_api_error(self, response: requests.Response):
        """Convert API error response to Python exception"""
        try:
            # Try parsing the error info according to spec and wrap it in an exception.
            info = response.json()
            exception = OpenEoApiError(
                http_status_code=response.status_code,
                code=info.get("code", "unknown"),
                message=info.get("message", "unknown error"),
                id=info.get("id"),
                url=info.get("url"),
            )
        except Exception:
            # When parsing went wrong: give minimal information.
            exception = OpenEoApiError(http_status_code=response.status_code, message=response.text)
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

    def delete(self, path) -> Response:
        """
        Do DELETE request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("delete", path=path)

    def patch(self, path) -> Response:
        """
        Do PATCH request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("patch", path=path)

    def put(self, path, headers: dict = None, data=None) -> Response:
        """
        Do PUT request to REST API.

        :param path: API path (without root url)
        :param headers: headers that gets added to the request.
        :param data: data that gets added to the request.
        :return: response: Response
        """
        return self.request("put", path=path, data=data, headers=headers)


class Connection(RestApiConnection):
    """
    Connection to an openEO backend.
    """

    def __init__(self, url, auth: AuthBase = None, session: requests.Session = None):
        """
        Constructor of Connection, authenticates user.
        :param url: String Backend root url
        """
        super().__init__(root_url=url, auth=auth, session=session)
        self._cached_capabilities = None

    def authenticate_basic(self, username: str, password: str) -> 'Connection':
        """
        Authenticate a user to the backend using basic username and password.
        :param username: User name
        :param password: User passphrase
        """
        resp = self.get(
            '/credentials/basic',
            # /credentials/basic is the only endpoint that expects a Basic HTTP auth
            auth=HTTPBasicAuth(username, password)
        ).json()
        # Switch to bearer based authentication in further requests.
        self.auth = BearerAuth(bearer=resp["access_token"])
        return self

    def authenticate_OIDC(self, client_id: str, webbrowser_open=None, timeout=120) -> 'Connection':
        """
        Authenticates a user to the backend using OpenID Connect.

        :param client_id: Client id to use for OpenID Connect authentication
        :param webbrowser_open: optional handler for the initial OAuth authentication request
            (opens a webbrowser by default)
        :param timeout: number of seconds after which to abort the authentication procedure
        """
        # Local import to avoid importing the whole OpenID Connect dependency chain. TODO: just do global import?
        from openeo.rest.auth.oidc import OidcAuthCodePkceAuthenticator

        # Per spec: '/credentials/oidc' will redirect to  OpenID Connect discovery document
        oidc_discovery_url = self.build_url('/credentials/oidc')
        authenticator = OidcAuthCodePkceAuthenticator(
            client_id=client_id,
            oidc_discovery_url=oidc_discovery_url,
            webbrowser_open=webbrowser_open,
            timeout=timeout,
        )
        # Do the Oauth/OpenID Connect flow and use the access token as bearer token.
        tokens = authenticator.get_tokens()
        # TODO: ability to refresh the token when expired?
        self.auth = BearerAuth(bearer=tokens.access_token)
        return self

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
        field = 'id' if self._api_version.at_least('0.4.0') else 'name'
        return [collection[field] for collection in self.list_collections() if field in collection]

    def capabilities(self) -> 'Capabilities':
        """
        Loads all available capabilities.

        :return: data_dict: Dict All available data types
        """
        if self._cached_capabilities is None:
            self._cached_capabilities = RESTCapabilities(self.get('/').json())

        return self._cached_capabilities

    @deprecated("Use 'list_output_formats' instead")
    def list_file_types(self) -> dict:
        return self.list_output_formats()

    def list_output_formats(self) -> dict:
        """
        Loads all available output formats.
        :return: data_dict: Dict All available output formats
        """
        return self.get('/output_formats').json()

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
        #TODO return service objects
        return self.get('/services').json()

    def describe_collection(self, name) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.
        :param name: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """
        return  self.get('/collections/{}'.format(name)).json()

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
        # TODO: Maybe format the result so that there get Job classes returned.
        """
        Lists all jobs of the authenticated user.
        :return: job_list: Dict of all jobs of the user.
        """
        return self.get('/jobs').json()["jobs"]

    def validate_processgraph(self, process_graph):
        # Endpoint: POST /validate
        raise NotImplementedError()

    def list_processgraphs(self, process_graph):
        # Endpoint: GET /process_graphs
        raise NotImplementedError()

    @property
    def _api_version(self):
        return self.capabilities().api_version_check

    def load_collection(self, collection_id: str, **kwargs) -> ImageCollectionClient:
        """
        Load an image collection by collection id

        see :py:meth:`openeo.rest.imagecollectionclient.ImageCollectionClient.load_collection`
        for available arguments.

        :param collection_id: image collection identifier (string)
        :return: ImageCollectionClient
        """
        return ImageCollectionClient.load_collection(collection_id=collection_id, session=self, **kwargs)

    # Legacy alias.
    imagecollection = load_collection

    def point_timeseries(self, graph, x, y, srs) -> dict:
        """Compute a timeseries for a given point location."""
        r = self.post("/timeseries/point?x={x}&y={y}&srs={s}".format(x=x, y=y, s=srs), graph)
        return r.json()

    def create_service(self, graph, type, **kwargs):
        kwargs["process_graph"] = graph
        kwargs["type"] = type
        response = self.post("/services", kwargs)
        return {
            'url': response.headers['Location'],
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
        response = self.get("/jobs/{}/results".format(job_id))
        return self.parse_json_response(response)

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

    # TODO: Maybe rename to execute and merge with execute().
    def download(self, graph, outputfile, format_options):
        """
        Downloads the result of a process graph synchronously, and save the result to the given file.
        This method is useful to export binary content such as images. For json content, the execute method is recommended.

        :param graph: Dict representing a process graph
        :param outputfile: output file
        :param format_options: formating options
        :return: job_id: String
        """
        path = "/preview"
        request = {
            "process_graph": graph
        }
        if self._api_version.at_least('0.4.0'):
            path = "/result"
        else:
            request["output"] = format_options

        download_url = self.build_url(path)

        r = self.post(download_url, json=request, stream=True, timeout=1000)
        with open(outputfile, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    def execute(self, process_graph, output_format, output_parameters=None, budget=None):
        """
        Execute a process graph synchronously.
        :param process_graph: Dict representing a process graph
        :param output_format: String Output format of the execution
        :param output_parameters: Dict of additional output parameters
        :param budget: Budget
        :return: job_id: String
        """
        # TODO: add output_format to execution
        path = "/preview"
        if self._api_version.at_least('0.4.0'):
            path = "/result"
        response = self.post(path, process_graph)
        return self.parse_json_response(response)

    def create_job(self, process_graph:Dict, output_format:str=None, output_parameters:Dict={},
                   title:str=None, description:str=None, plan:str=None, budget=None,
                   additional:Dict={}):
        """
        Posts a job to the back end.
        :param process_graph: String data of the job (e.g. process graph)
        :param output_format: String Output format of the execution - DEPRECATED in 0.4.0
        :param output_parameters: Dict of additional output parameters - DEPRECATED in 0.4.0
        :param title: String title of the job
        :param description: String description of the job
        :param budget: Budget
        :return: job_id: String Job id of the new created job
        """

        process_graph = {
             "process_graph": process_graph,
             "title": title,
             "description": description,
             "plan": plan,
             "budget": budget
         }

        if not self._api_version.at_least('0.4.0'):
            process_graph["output"] = {
                "format": output_format,
                "parameters": output_parameters
            }

        job_status = self.post("/jobs", process_graph)

        job = None

        if job_status.status_code == 201:
            job_info = job_status.headers._store
            if "openeo-identifier" in job_info:
                job_id = job_info['openeo-identifier'][1]
                job = RESTJob(job_id, self)
            elif "location" in job_info:
                job_id = job_info['location'][1].split("/")[-1]
                job = RESTJob(job_id, self)
        else:
            self._handle_error_response(job_status)

        return job

    def parse_json_response(self, response: requests.Response):
        """
        Parses json response, if an error occurs it raises an Exception.
        :param response: Response of a RESTful request
        :return: response: JSON Response
        """
        if response.status_code == 200 or response.status_code == 201:
            return response.json()
        else:
            self._handle_error_response(response)

    def _handle_error_response(self, response):
        # TODO replace this with `_raise_api_error`
        if response.status_code == 502:
            from requests.exceptions import ProxyError
            raise ProxyError("The proxy returned an error, this could be due to a timeout.")
        else:
            message = None
            if response.headers['Content-Type'] == 'application/json':
                message = response.json().get('message', None)
            if message:
                message = response.text

            raise ConnectionAbortedError(message)

    def get_outputformats(self) -> dict:
        """
        Loads all available output formats.

        :return: data_dict: Dict All available output formats
        """
        raise NotImplementedError()

    def load_disk_collection(self, format: str, glob_pattern: str, options: dict = {}) -> ImageCollectionClient:
        """
        Loads image data from disk as an ImageCollection.

        :param format: the file format, e.g. 'GTiff'
        :param glob_pattern: a glob pattern that matches the files to load from disk
        :param options: options specific to the file format
        :return: the data as an ImageCollection
        """
        return ImageCollectionClient.load_disk_collection(self, format, glob_pattern, **options)


def connect(url, auth_type: str = None, auth_options: dict = {}, session: requests.Session = None) -> Connection:
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
    :rtype: openeo.connections.Connection
    """
    connection = Connection(url, session=session)
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


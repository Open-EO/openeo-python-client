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

from openeo.capabilities import Capabilities
from openeo.imagecollection import CollectionMetadata
from openeo.rest.auth.auth import NullAuth, BearerAuth
from openeo.rest.job import RESTJob
from openeo.rest.rest_capabilities import RESTCapabilities

_log = logging.getLogger(__name__)


class Connection:

    def __init__(self, url, auth=None):
        """
        Constructor of RESTConnection, authenticates user.
        :param url: String Backend endpoint url
        """
        self.endpoint = url
        # TODO what is the point of this `root`? Isn't `url`/`self.endpoint` enough
        self.root = ""
        self._cached_capabilities = None
        self.auth = auth or NullAuth()

    def authenticate_basic(self, username: str, password: str) -> 'RESTConnection':
        """
        Authenticate a user to the backend using basic username and password.
        :param username: User name
        :param password: User passphrase
        """
        resp = self.get(
            self.root + '/credentials/basic',
            # /credentials/basic is the only endpoint that expects a Basic HTTP auth
            auth=HTTPBasicAuth(username, password)
        ).json()
        # Switch to bearer based authentication in further requests.
        self.auth = BearerAuth(bearer=resp["access_token"])
        return self

    def authenticate_OIDC(self, client_id: str, webbrowser_open=None) -> 'RESTConnection':
        """
        Authenticates a user to the backend using OpenID Connect.

        :param client_id: Client id to use for OpenID Connect authentication
        :param webbrowser_open: optional handler for the initial OAuth authentication request
            (opens a webbrowser by default)
        """
        # Local import to avoid importing the whole OpenID Connect dependency chain. TODO: just do global import?
        from openeo.rest.auth.oidc import OidcAuthCodePkceAuthenticator

        # Per spec: '/credentials/oidc' will redirect to  OpenID Connect discovery document
        oidc_discovery_url = self._url_join(self.endpoint, '/credentials/oidc')
        authenticator = OidcAuthCodePkceAuthenticator(
            client_id=client_id,
            oidc_discovery_url=oidc_discovery_url,
            webbrowser_open=webbrowser_open
        )
        # Do the Oauth/OpenID Connect flow
        tokens = authenticator.get_tokens()
        # TODO: ability to refresh the token when expired?
        self.auth = BearerAuth(bearer=tokens.access_token)
        return self

    def describe_account(self) -> str:
        """
        Describes the currently authenticated user account.
        """
        info = self.get(self.root + '/me')
        return self.parse_json_response(info)

    def user_jobs(self) -> dict:
        """
        Loads all jobs of the current user.
        :return: jobs: Dict All jobs of the user
        """
        return self.get(self.root + '/jobs').json()["jobs"]

    def list_collections(self) -> List[dict]:
        """
        Loads all available imagecollections types.
        :return: list of collection meta data dictionaries
        """
        return self.get(self.root + '/collections').json()["collections"]

    def list_collection_ids(self) -> List[str]:
        """
        Get list of all collection ids
        :return: list of collection ids
        """
        field = 'id' if self._api_version.at_least('0.4.0') else 'name'
        return [collection[field] for collection in self.list_collections() if field in collection]

    @deprecated("Use list_processes")
    def get_processes(self):
        """
        EXPERIMENTAL
        Returns processes of back end.
        :return: data_dict: Dict All available data types
        """
        # TODO return only provided processes of the back end
        from openeo.rest.rest_processes import RESTProcesses
        return RESTProcesses(self)

    def capabilities(self) -> 'Capabilities':
        """
        Loads all available capabilities.

        :return: data_dict: Dict All available data types
        """
        if self._cached_capabilities is None:
            self._cached_capabilities = RESTCapabilities(self.get(self.root + '/').json())

        return self._cached_capabilities

    def list_file_types(self) -> dict:
        """
        Loads all available output formats.
        :return: data_dict: Dict All available output formats
        """
        return self.get(self.root + '/output_formats').json()["formats"]

    def list_service_types(self) -> dict:
        """
        Loads all available service types.
        :return: data_dict: Dict All available service types
        """
        return self.get(self.root + '/service_types').json()

    def list_services(self) -> dict:
        """
        Loads all available services of the authenticated user.
        :return: data_dict: Dict All available service types
        """
        #TODO return service objects
        return self.get(self.root + '/services').json()

    def describe_collection(self, name) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.
        :param name: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """
        return  self.get(self.root + '/collections/{}'.format(name)).json()

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
        return self.not_supported()

    def list_processgraphs(self, process_graph):

        # Endpoint: GET /process_graphs
        return self.not_supported()

    @property
    def _api_version(self):
        return self.capabilities().api_version_check

    def imagecollection(self, image_collection_id) -> 'ImageCollection':
        """
        Get imagecollection by id.
        :param image_collection_id: String image collection identifier
        :return: collection: ImageCollectionClient the imagecollection with the id
        """
        assert self._api_version.at_least('0.4.0')
        # TODO avoid local imports
        from .imagecollectionclient import ImageCollectionClient
        image = ImageCollectionClient.load_collection(image_collection_id, self)
        return image

    def point_timeseries(self, graph, x, y, srs) -> dict:
        """Compute a timeseries for a given point location."""
        r = self.post(self.root + "/timeseries/point?x={x}&y={y}&srs={s}".format(x=x, y=y, s=srs), graph)
        return r.json()

    def create_service(self, graph, type, **kwargs):
        kwargs["process_graph"] = graph
        kwargs["type"] = type

        response = self.post(self.root + "/services", kwargs)

        if response.status_code == 201:
            service_url = response.headers['location']

            return {
                'url': service_url
            }
        else:
            self._handle_error_response(response)

    def remove_service(self, service_id: str):
        """
        Stop and remove a secondary web service.
        :param service_id: service identifier
        :return:
        """
        response = self.delete('/services/' + service_id)
        if response.status_code != 204:
            self._handle_error_response(response)

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
        return self.not_supported()

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

        download_url = self.endpoint + self.root + path

        # TODO: why not self.post()?
        r = requests.post(download_url, json=request, stream = True, timeout=1000 )
        if r.status_code == 200:
            with open(outputfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        else:
            self._handle_error_response(r)

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
        response = self.post(self.root + path, process_graph)
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

    def post(self, path, postdata) -> Response:
        """
        Makes a RESTful POST request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param postdata: Data of the post request
        :return: response: Response
        """
        # TODO: add .raise_for_status() by default?
        url = self._url_join(self.endpoint, path)
        return requests.post(url, json=postdata, auth=self.auth)

    def delete(self, path) -> Response:
        """
        Makes a RESTful DELETE request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :return: response: Response
        """
        # TODO: add .raise_for_status() by default?
        url = self._url_join(self.endpoint, path)
        return requests.delete(url, auth=self.auth)

    def patch(self, path) -> Response:
        """
        Makes a RESTful PATCH request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :return: response: Response
        """
        url = self._url_join(self.endpoint, path)
        return requests.patch(url, auth=self.auth)

    def put(self, path, header={}, data=None) -> Response:
        """
        Makes a RESTful PUT request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param header: header that gets added to the request.
        :param data: data that gets added to the request.
        :return: response: Response
        """
        url = self._url_join(self.endpoint, path)
        return requests.put(url, headers=header, data=data, auth=self.auth)

    def get(self, path, stream=False, check_status=True, auth: AuthBase = None) -> Response:
        """
        Makes a RESTful GET request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param stream: True if the get request should be streamed, else False
        :param check_status: whether to check the status code
        :param auth: optional custom authentication to use instead of the default one
        :return: response: Response
        """
        url = self._url_join(self.endpoint, path)
        resp = requests.get(url, stream=stream, auth=auth or self.auth)
        if check_status:
            # TODO: raise a custom OpenEO branded exception?
            resp.raise_for_status()
        return resp

    def _url_join(self, base: str, path: str):
        """Join a base url and sub path properly"""
        return urljoin(base.rstrip('/') + '/', path.lstrip('/'))

    def get_outputformats(self) -> dict:
        """
        Loads all available output formats.

        :return: data_dict: Dict All available output formats
        """
        return self.not_supported()

    def not_supported(self):
        # TODO why not raise exception? the print isn't even to standard error
        # TODO: also: is this about not supporting YET (feature under construction) or impossible to support?
        not_support = "This function is not supported by the python client yet."
        print(not_support)
        return not_support


def connect(url, auth_type: str = None, auth_options: dict = {}) -> Connection:
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
    connection = Connection(url)
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


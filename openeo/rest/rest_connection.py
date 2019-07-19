import logging
import shutil
from typing import Dict, List

import requests

from openeo.auth.auth_basic import BasicAuth
from openeo.auth.auth_none import NoneAuth
from openeo.capabilities import Capabilities
from openeo.connection import Connection
from openeo.rest.job import RESTJob
from openeo.rest.rest_capabilities import RESTCapabilities
from openeo.rest.rest_processes import RESTProcesses

"""
This module provides a Connection object to manage and persist settings when interacting with the OpenEO API.
"""

_log = logging.getLogger(__name__)


class RESTConnection(Connection):

    def __init__(self, url, auth_type=NoneAuth, auth_options={}):
        # TODO: Maybe in future only the endpoint is needed, because of some kind of User object inside of the connection.
        """
        Constructor of RESTConnection, authenticates user.
        :param url: String Backend endpoint url
        :param username: String Username credential of the user
        :param password: String Password credential of the user
        :param auth_class: Auth instance of the abstract Auth class
        :return: token: String Bearer token
        """
        # TODO what is the point of this `root`? Isn't `url`/`self.endpoint` enough
        self.root = ""
        self._cached_capabilities = None
        self.connect(url, auth_type, auth_options)

    def connect(self, url, auth_type=NoneAuth, auth_options={}) -> bool:
        """
        Authenticates a user to the backend using auth class.
        :param url: String Backend endpoint url
        :param username: String Username credential of the user
        :param password: String Password credential of the user
        :param auth_class: Auth instance of the abstract Auth class
        :return: token: String Bearer token
        """

        username = auth_options.get('username')
        password = auth_options.get('password')

        self.userid = username
        self.endpoint = url

        if auth_type == NoneAuth and username and password:
            auth_type = BasicAuth

        self.authent = auth_type(username, password, self.endpoint)

        status = self.authent.login()

        return status

    def authenticate_OIDC(self, options={}) -> str:
        """
        Authenticates a user to the backend using OIDC.
        :param options: Authentication options
        """
        return self.not_supported()

    def authenticate_basic(self, username, password):
        """
        Authenticates a user to the backend using HTTP Basic.
        :param username: User name
        :param password: User passphrase
        """

        if username and password:
            self.authent = BasicAuth(username, password, self.endpoint)

        # return self.not_supported()

    def describe_account(self) -> str:
        """
        Describes the currently authenticated user account.
        """
        info = self.get(self.root + '/me')
        return self.parse_json_response(info)

    def user_jobs(self) -> dict:
        #TODO: Create a kind of User class to abstract the information (e.g. userid, username, password from the connection.
        #TODO: Move information to Job class and return a list of Jobs.
        """
        Loads all jobs of the current user.
        :return: jobs: Dict All jobs of the user
        """
        # TODO: self.userid might be None
        jobs = self.get(self.root + '/users/{}/jobs'.format(self.userid))
        return self.parse_json_response(jobs)

    def list_collections(self) -> List[dict]:
        """
        Loads all available imagecollections types.
        :return: list of collection meta data dictionaries
        """
        data = self.get(self.root + '/collections', auth=False)
        response = self.parse_json_response(data)
        return response["collections"]

    def list_collection_ids(self) -> List[str]:
        """
        Get list of all collection ids
        :return: list of collection ids
        """
        field = 'id' if self._api_version.at_least('0.4.0') else 'name'
        return [collection[field] for collection in self.list_collections() if field in collection]

    def get_processes(self):
        """
        EXPERIMENTAL
        Returns processes of back end.
        :return: data_dict: Dict All available data types
        """
        # TODO return only provided processes of the back end

        return RESTProcesses(self)

    def capabilities(self) -> 'Capabilities':
        """
        Loads all available capabilities.

        :return: data_dict: Dict All available data types
        """
        if self._cached_capabilities is None:
            data = self.get(self.root + '/', auth=False)
            data.raise_for_status()
            self._cached_capabilities = RESTCapabilities(self.parse_json_response(data))

        return self._cached_capabilities

    def list_file_types(self) -> dict:
        """
        Loads all available output formats.
        :return: data_dict: Dict All available output formats
        """
        data = self.get(self.root + '/output_formats', auth=False)
        return self.parse_json_response(data)

    def list_service_types(self) -> dict:
        """
        Loads all available service types.
        :return: data_dict: Dict All available service types
        """
        data = self.get(self.root + '/service_types', auth=False)
        return self.parse_json_response(data)

    def list_services(self) -> dict:
        """
        Loads all available services of the authenticated user.
        :return: data_dict: Dict All available service types
        """
        #TODO return service objects
        data = self.get(self.root + '/services', auth=True)
        return self.parse_json_response(data)

    def describe_collection(self, name) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.
        :param name: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """
        if name:
            data_info = self.get(self.root + '/collections/{}'.format(name), auth=False)
            return self.parse_json_response(data_info)
        else:
            raise ValueError("Invalid argument col_id: {}".format(str(name)))

    def list_processes(self) -> dict:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        """
        Loads all available processes of the back end.
        :return: processes_dict: Dict All available processes of the back end.
        """
        processes = self.get('/processes', auth=False)

        response = self.parse_json_response(processes)

        if "processes" in response:
            return response["processes"]

        return response

    def list_jobs(self) -> dict:
        # TODO: Maybe format the result so that there get Job classes returned.
        """
        Lists all jobs of the authenticated user.
        :return: job_list: Dict of all jobs of the user.
        """
        jobs = self.get('/jobs', auth=True)
        return self.parse_json_response(jobs)

    def validate_processgraph(self, process_graph):

        # Endpoint: POST /validate
        return self.not_supported()

    def list_processgraphs(self, process_graph):

        # Endpoint: GET /process_graphs
        return self.not_supported()

    def get_process(self, process_id) -> dict:
        # TODO: Maybe create some kind of Process class.
        """
        Get detailed information about a specifig process.
        :param process_id: String Process identifier
        :return: processes_dict: Dict with the detail information about the
                                 process
        """
        if process_id:
            process_info = self.get('/processes/{}'.format(process_id), auth=False)
            processes_dict = self.parse_json_response(process_info)
        else:
            processes_dict = None

        return processes_dict

    @property
    def _api_version(self):
        return self.capabilities().api_version_check

    def imagecollection(self, image_collection_id) -> 'ImageCollection':
        """
        Get imagecollection by id.
        :param image_collection_id: String image collection identifier
        :return: collection: RestImageCollection the imagecollection with the id
        """
        if self._api_version.at_least('0.4.0'):
            return self._image_040(image_collection_id)
        else:
            from .imagery import RestImagery
            collection = RestImagery({'name': image_collection_id, 'process_id': 'get_collection'}, self)

            self.fetch_metadata(image_collection_id, collection)
            return collection

    def _image_040(self, image_product_id) -> 'ImageCollection':
        """
        Get imagery by id.
        :param image_collection_id: String image collection identifier
        :return: collection: RestImagery the imagery with the id
        """
        #new implementation: https://github.com/Open-EO/openeo-api/issues/160
        # TODO avoid local imports
        from .imagecollectionclient import ImageCollectionClient
        image = ImageCollectionClient.create_collection(image_product_id, self)
        self.fetch_metadata(image_product_id, image)
        return image

    def image(self, image_product_id) -> 'ImageCollection':
        """
        Get imagery by id.
        DEPRECATED

        :param image_collection_id: String image collection identifier
        :return: collection: RestImagery the imagery with the id
        """
        # TODO avoid local imports
        from .rest_processes import RESTProcesses

        image = RESTProcesses( self)

        self.fetch_metadata(image_product_id, image)
        return image

    def fetch_metadata(self, image_product_id, image_collection):
        # TODO: this sets public properties on image_collection: shouldn't this be part of ImageCollection class then?
        # read and format extent, band and date availability information
        data_info = self.describe_collection(image_product_id)
        image_collection.bands = []
        if data_info:
            if 'bands' in data_info:
                for band in data_info['bands']: image_collection.bands.append(band['band_id'])
            image_collection.dates = data_info.get('time',[])
            image_collection.extent = data_info.get('extent',None)
        else:
            image_collection.bands = ['not specified']
            image_collection.dates = ['not specified']
            image_collection.extent = ['not specified']

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
        response = self.delete('/services/' + service_id)
        if response.status_code != 204:
            self._handle_error_response(response)

    def job_results(self, job_id):
        response = self.get("/jobs/{}/results".format(job_id))
        return self.parse_json_response(response)

    def list_files(self, user_id=None):
        """
        Lists all files that the logged in user uploaded.
        :param user_id: user id, which files should be listed.
        :return: file_list: List of the user uploaded files.
        """

        if not user_id:
            user_id = self.userid

        files = self.get('/files/{}'.format(user_id))

        #TODO: Create File Objects.

        return self.parse_json_response(files)

    def create_file(self, path, user_id=None):
        """
        Creates virtual file
        :param user_id: owner of the file.
        :return: file object.
        """
        # No endpoint just returns a file object.
        return self.not_supported()

    # TODO: Maybe rename to execute and merge with execute().
    def download(self, graph, time, outputfile, format_options):
        """
        Downloads the result of a process graph synchronously, and save the result to the given file.
        This method is useful to export binary content such as images. For json content, the execute method is recommended.

        :param graph: Dict representing a process graph
        :param time: dba
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

    def post(self, path, postdata):
        """
        Makes a RESTful POST request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param postdata: Data of the post request
        :return: response: Response
        """

        auth_header = self.authent.get_header()
        auth = self.authent.get_auth()
        # TODO: add .raise_for_status() by default?
        return requests.post(self.endpoint+path, json=postdata, headers=auth_header, auth=auth)

    def delete(self, path):
        """
        Makes a RESTful DELETE request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :return: response: Response
        """

        auth_header = self.authent.get_header()
        auth = self.authent.get_auth()
        # TODO: add .raise_for_status() by default?
        return requests.delete(self.endpoint+path, headers=auth_header, auth=auth)

    def patch(self, path):
        """
        Makes a RESTful PATCH request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :return: response: Response
        """
        auth_header = self.authent.get_header()
        auth = self.authent.get_auth()
        return requests.patch(self.endpoint+path, headers=auth_header, auth=auth)

    def put(self, path, header={}, data=None):
        """
        Makes a RESTful PUT request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param header: header that gets added to the request.
        :param data: data that gets added to the request.
        :return: response: Response
        """
        auth_header = self.authent.get_header()

        # Merge headers
        head = auth_header.copy()
        head.update(header)

        auth = self.authent.get_auth()

        if data:
            return requests.put(self.endpoint+path, headers=head, data=data, auth=auth)
        else:
            return requests.put(self.endpoint+path, headers=head, auth=auth)

    def get(self,path, stream=False, auth=True):
        """
        Makes a RESTful GET request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param stream: True if the get request should be streamed, else False
        :param auth: True if the get request should be authenticated, else False
        :return: response: Response
        """

        if auth:
            auth_header = self.authent.get_header()
            auth = self.authent.get_auth()
        else:
            auth_header = {}
            auth = None
        # TODO: add .raise_for_status() by default?
        return requests.get(self.endpoint+path, headers=auth_header, stream=stream, auth=auth)

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


def connection(url, auth_type=NoneAuth, auth_options={}) -> RESTConnection:
    """
    This method is the entry point to OpenEO. You typically create one connection object in your script or application, per back-end.
    and re-use it for all calls to that backend.
    If the backend requires authentication, you should set pass your credentials.

    :param endpoint: The http url of an OpenEO endpoint.

    :rtype: openeo.connections.Connection
    """

    return RESTConnection(url, auth_type, auth_options)


def session(userid=None, endpoint: str = "https://openeo.org/openeo") -> RESTConnection:
    """
    Deprecated, use openeo.connect
    This method is the entry point to OpenEO. You typically create one session object in your script or application, per back-end.
    and re-use it for all calls to that backend.
    If the backend requires authentication, you should set pass your credentials.
    :param endpoint: The http url of an OpenEO endpoint.
    :rtype: openeo.sessions.Session
    """
    return connection(url=endpoint)

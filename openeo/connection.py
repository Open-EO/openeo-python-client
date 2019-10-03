"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Connection object to manage and persist settings when interacting with the OpenEO API.
"""

from abc import ABC, abstractmethod
from typing import Dict

from openeo.capabilities import Capabilities
from openeo.imagecollection import ImageCollection
from openeo.job import Job


# TODO is it necessary to have this abstract base class Connection
#      when there is just this one RESTConnection implementation class?

class Connection(ABC):
    """
    A `Connection` class represents a connection with an OpenEO service.
    It is your entry point to create new Image Collections.
    """


    @abstractmethod
    def capabilities(self) -> Capabilities:
        """
        Loads all available capabilities.

        :return: data_dict: Dict All available data types
        """

    @abstractmethod
    def list_file_types(self) -> dict:
        """
        Loads all available output formats.
        :return: data_dict: Dict All available output formats
        """

    @abstractmethod
    def list_service_types(self) -> dict:
        """
        Loads all available service types.
        :return: data_dict: Dict All available service types
        """

    @abstractmethod
    def list_collections(self) -> dict:
        """
        Retrieve all collections available in the backend.

        :return: a dict containing collection information. The 'product_id' corresponds to an image collection id.
        """
        pass

    @abstractmethod
    def describe_collection(self, name) -> dict:
        """
        Loads detailed information of a specific image collection.
        :param name: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """

    @abstractmethod
    def list_processes(self) -> dict:
        """
        Loads all available processes of the back end.
        :return: processes_dict: Dict All available processes of the back end.
        """

    @abstractmethod
    def authenticate_basic(self, username: str, password: str):
        """
        Authenticate a user to the backend using basic username and password.
        :param options: Authentication options
        """

    @abstractmethod
    def authenticate_OIDC(self, client_id: str):
        """
        Authenticates a user to the backend using OpenID Connect.
        :param client_id: Authentication options
        """

    @abstractmethod
    def describe_account(self) -> str:
        """
        Describes the currently authenticated user account.
        """

    @abstractmethod
    def list_files(self):
        """
        Lists all files that the logged in user uploaded.
        :return: file_list: List of the user uploaded files.
        """

    @abstractmethod
    def create_file(self, path):
        """
        Creates virtual file
        :return: file object.
        """

    @abstractmethod
    def validate_processgraph(self, process_graph):
        pass

    @abstractmethod
    def list_processgraphs(self, process_graph):
        pass

    @abstractmethod
    def execute(self, process_graph, output_format, output_parameters=None, budget=None):
        """
        Execute a process graph synchronously.
        :param process_graph: Dict representing a process graph
        :param output_format: String Output format of the execution
        :param output_parameters: Dict of additional output parameters
        :param budget: Budget
        :return: job_id: String
        """
        pass

    @abstractmethod
    def list_jobs(self) -> dict:
        """
        Lists all jobs of the authenticated user.
        :return: job_list: Dict of all jobs of the user.
        """
        pass

    @abstractmethod
    def create_job(self, process_graph:Dict, output_format:str=None, output_parameters:Dict={},
                   title:str=None, description:str=None, plan:str=None, budget=None,
                   additional:Dict={}) -> Job:
        """
        Posts a job to the back end.

        :param process_graph: String data of the job (e.g. process graph)
        :param output_format: String Output format of the execution
        :param output_parameters: Dict of additional output parameters
        :param title: String title of the job
        :param description: String description of the job
        :param budget: Budget
        :return: job_id: Job object representing the job
        """

        pass

    @abstractmethod
    def list_services(self) -> dict:
        """
        Loads all available services of the authenticated user.
        :return: data_dict: Dict All available service types
        """
        pass

    @abstractmethod
    def create_service(self, graph, **kwargs) -> dict:
        """
        Create a secondary web service such as WMTS, TMS or WCS. The underlying data is processes on-demand, but a process graph may simply access results from a batch job.

        :param graph: Dict representing a process graph
        :param type: The type of service, e.g. 'WMTS', 'TMS' or 'WCS'. A list of supported types can be queried from the backend.
        :param title: A short description to easily distinguish entities.
        :param description: Detailed description to fully explain the entity. CommonMark 0.28 syntax MAY be used for rich text representation.
        :return: A dict containing service details.
        """
        pass

    @abstractmethod
    def remove_service(self, service_id: str):
        """
        Stop and remove a secondary web service.
        :param service_id: service identifier
        :return:
        """
        pass


# TODO: methods below are depricated / should be located somewhere else.

    @abstractmethod
    def imagecollection(self, image_collection_id:str) -> ImageCollection:
        """
        Retrieves an Image Collection object based on the id of a given layer.
        A list of available collections can be retrieved with :meth:`openeo.connection.Connection.list_collections`.

        :param image_collection_id: The id of the image collection to retrieve.

        :rtype: openeo.imagecollection.ImageCollection
        """
        pass

    @abstractmethod
    def download(self, graph, outputfile, format_options):
        """
        Downloads a result of a process graph synchronously.

        :param graph: Dict representing a process graph
        :param outputfile: output file
        :param format_options: formating options

        :return: job_id: String
        """

    @abstractmethod
    def get_outputformats(self) -> dict:
        """
        Loads all available output formats.

        :return: data_dict: Dict All available output formats
        """
        pass

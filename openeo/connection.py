from abc import ABC, abstractmethod
from .auth import Auth
from openeo.imagecollection import ImageCollection
from.capabilities import Capabilities
from .job import Job
from typing import Dict

"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Connection object to manage and persist settings when interacting with the OpenEO API.
"""


class Connection(ABC):
    """
    A `Connection` class represents a connection with an OpenEO service.
    It is your entry point to create new Image Collections.
    """

    @property
    @abstractmethod
    def connect(self, username, password, auth:Auth) -> bool:
        """
        Authenticates a user to the backend using auth class.

        :param username: String Username credential of the user
        :param password: String Password credential of the user
        :param auth_class: Auth instance of the abstract Auth class

        :return: token: String Bearer token
        """
        pass

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
    def authenticate_OIDC(self, options={}) -> str:
        """
        Authenticates a user to the backend using OIDC.
        :param options: Authentication options
        """

    @abstractmethod
    def authenticate_basic(self, username, password) -> str:
        """
        Authenticates a user to the backend using HTTP Basic.
        :param options: Authentication options
        """

    @abstractmethod
    def describe_account(self) -> str:
        """
        Describes the currently authenticated user account.
        """

    @abstractmethod
    def list_files(self, user_id=None):
        """
        Lists all files that the logged in user uploaded.
        :param user_id: user id, which files should be listed.
        :return: file_list: List of the user uploaded files.
        """

    @abstractmethod
    def create_file(self, path, user_id=None):
        """
        Creates virtual file
        :param user_id: owner of the file.
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
    def get_process(self, process_id) -> dict:
        # TODO: Maybe create some kind of Process class.
        """
        Get detailed information about a specifig process.

        :param process_id: String Process identifier

        :return: processes_dict: Dict with the detail information about the
                                 process
        """
        pass

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
    def image(self, image_product_id) -> 'ImageCollection':
        """
        Get imagery by id.
        DEPRECATED

        :param image_collection_id: String image collection identifier

        :return: collection: RestImagery the imagery with the id
        """

    @abstractmethod
    def download(self, graph, time, outputfile, format_options):
        """
        Downloads a result of a process graph synchronously.

        :param graph: Dict representing a process graph
        :param time: dba
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

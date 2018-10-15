from abc import ABC, abstractmethod
from .auth import Auth
from .imagecollection import ImageCollection

"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Session object to manage and persist settings when interacting with the OpenEO API.
"""


class Session(ABC):
    """
    A `Session` class represents a connection with an OpenEO service. It is your entry point to create new Image Collections.
    """

    @property
    @abstractmethod
    def auth(self, username, password, auth:Auth) -> bool:
        """
        Authenticates a user to the backend using auth class.

        :param username: String Username credential of the user
        :param password: String Password credential of the user
        :param auth_class: Auth instance of the abstract Auth class

        :return: token: String Bearer token
        """
        pass

    @abstractmethod
    def list_capabilities(self) -> dict:
        """
        Loads all available capabilities.

        :return: data_dict: Dict All available data types
        """

    @abstractmethod
    def list_collections(self) -> dict:
        """
        Retrieve all collections available in the backend.

        :return: a dict containing collection information. The 'product_id' corresponds to an image collection id.
        """
        pass

    @abstractmethod
    def get_collection(self, col_id) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.

        :param col_id: String Id of the collection

        :return: data_dict: Dict Detailed information about the collection
        """

    @abstractmethod
    def get_all_processes(self) -> dict:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        """
        Loads all available processes of the back end.

        :return: processes_dict: Dict All available processes of the back end.
        """

    @abstractmethod
    def get_process(self, process_id) -> dict:
        # TODO: Maybe create some kind of Process class.
        """
        Get detailed information about a specifig process.

        :param process_id: String Process identifier

        :return: processes_dict: Dict with the detail information about the
                                 process
        """

    @abstractmethod
    def create_job(self, post_data, evaluation="lazy") -> str:
        # TODO: Create a Job class or something for the creation of a nested process execution...
        """
        Posts a job to the back end including the evaluation information.

        :param post_data: String data of the job (e.g. process graph)
        :param evaluation: String Option for the evaluation of the job

        :return: job_id: String Job id of the new created job
        """

    @abstractmethod
    def imagecollection(self, image_collection_id:str) -> ImageCollection:
        """
        Retrieves an Image Collection object based on the id of a given layer.
        A list of available collections can be retrieved with :meth:`openeo.sessions.Session.list_collections`.

        :param image_collection_id: The id of the image collection to retrieve.

        :rtype: openeo.imagecollection.ImageCollection
        """
        pass

    @abstractmethod
    def image(self, image_product_id) -> 'ImageCollection':
        """
        Get imagery by id.

        :param image_collection_id: String image collection identifier

        :return: collection: RestImagery the imagery with the id
        """

    @abstractmethod
    def user_jobs(self) -> dict:
        #TODO: Create a kind of User class to abstract the information (e.g. userid, username, password from the session.
        """
        Loads all jobs of the current user.

        :return: jobs: Dict All jobs of the user
        """

    @abstractmethod
    def user_download_file(self, file_path, output_file):
        """
        Downloads a user file to the back end.

        :param file_path: remote path to the file that should be downloaded.
        :param output_file: local path, where the file should be saved.

        :return: status: True if it was successful, False otherwise
        """

    @abstractmethod
    def user_upload_file(self, file_path, remote_path=None):
        """
        Uploads a user file to the back end.

        :param file_path: Local path to the file that should be uploaded.
        :param remote_path: Remote path of the file where it should be uploaded.

        :return: status: True if it was successful, False otherwise
        """

    @abstractmethod
    def user_delete_file(self, file_path):
        """
        Deletes a user file in the back end.

        :param file_path: remote path to the file that should be deleted.
        :return: status: True if it was successful, False otherwise
        """
        pass

    @abstractmethod
    def user_list_files(self):
        """
        Lists all files that the logged in user uploaded.

        :return: file_list: List of the user uploaded files.
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

    @abstractmethod
    def create_service(self, graph, type="WMTS", title="", description="") -> dict:
        """
        Create a secondary web service such as WMTS, TMS or WCS. The underlying data is processes on-demand, but a process graph may simply access results from a batch job.

        :param graph: Dict representing a process graph
        :param type: The type of service, e.g. 'WMTS', 'TMS' or 'WCS'. A list of supported types can be queried from the backend.
        :param title: A short description to easily distinguish entities.
        :param description: Detailed description to fully explain the entity. CommonMark 0.28 syntax MAY be used for rich text representation.
        :return: A dict containing service details.
        """
        pass
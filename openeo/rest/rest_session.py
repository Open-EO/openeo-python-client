from urllib.parse import urlparse
import shutil
import os

import requests

from ..auth.auth_none import NoneAuth

import json
from ..sessions import Session

"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Session object to manage and persist settings when interacting with the OpenEO API.
"""


class RESTSession(Session):

    def __init__(self,userid, endpoint):
        # TODO: Maybe in future only the endpoint is needed, because of some kind of User object inside of the session.
        """
        Constructor of RESTSession
        :param userid: String User login credential
        :param endpoint: String Backend endpoint url
        """
        self.userid = userid
        self.endpoint = endpoint
        self.root = ""
        self.authent = NoneAuth("none", "none", endpoint)

    def auth(self, username, password, auth_class=NoneAuth) -> bool:
        """
        Authenticates a user to the backend using auth class.
        :param username: String Username credential of the user
        :param password: String Password credential of the user
        :param auth_class: Auth instance of the abstract Auth class
        :return: token: String Bearer token
        """

        self.authent = auth_class(username, password, self.endpoint)
        status = self.authent.login()

        return status

    def user_jobs(self) -> dict:
        #TODO: Create a kind of User class to abstract the information (e.g. userid, username, password from the session.
        #TODO: Move information to Job class and return a list of Jobs.
        """
        Loads all jobs of the current user.
        :return: jobs: Dict All jobs of the user
        """
        jobs = self.get(self.root + '/users/{}/jobs'.format(self.userid))
        return self.parse_json_response(jobs)

    def list_collections(self) -> dict:
        """
        Loads all available imagecollections types.
        :return: data_dict: Dict All available data types
        """
        data = self.get(self.root + '/data', auth=False)
        return self.parse_json_response(data)

    def list_capabilities(self) -> dict:
        """
        Loads all available capabilities.

        :return: data_dict: Dict All available data types
        """
        data = self.get(self.root + '/capabilities', auth=False)
        return self.parse_json_response(data)

    def get_outputformats(self) -> dict:
        """
        Loads all available output formats.
        :return: data_dict: Dict All available output formats
        """
        data = self.get(self.root + '/capabilities/output_formats', auth=False)
        return self.parse_json_response(data)

    def get_collection(self, col_id) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.
        :param col_id: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """
        if col_id:
            data_info = self.get(self.root + '/data/{}'.format(col_id), auth=False)
            return self.parse_json_response(data_info)
        else:
            raise ValueError("Invalid argument col_id: "+ str(col_id))

    def get_all_processes(self) -> dict:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        """
        Loads all available processes of the back end.
        :return: processes_dict: Dict All available processes of the back end.
        """
        processes = self.get('/processes', auth=False)
        return self.parse_json_response(processes)

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

    def create_job(self, post_data, evaluation="lazy") -> str:
        # TODO: Create a Job class or something for the creation of a nested process execution...
        """
        Posts a job to the back end including the evaluation information.
        :param post_data: String data of the job (e.g. process graph)
        :param evaluation: String Option for the evaluation of the job
        :return: job_id: String Job id of the new created job
        """
        job_status = self.post("/jobs?evaluate={}".format(evaluation), post_data)

        if job_status.status_code == 200:
            job_info = json.loads(job_status.text)
            if 'job_id' in job_info:
                job_id = job_info['job_id']
        else:
            job_id = None

        return job_id

    def imagecollection(self, image_collection_id) -> 'ImageCollection':
        """
        Get imagecollection by id.
        :param image_collection_id: String image collection identifier
        :return: collection: RestImageCollection the imagecollection with the id
        """
        from .imagery import RestImagery
        collection = RestImagery({'collection_id': image_collection_id}, self)

        # read and format extent, band and date availability information
        data_info = self.get_collection(image_collection_id)
        collection.bands = []
        if data_info:
            if "bands" in data_info:
                for band in data_info['bands']: collection.bands.append(band['band_id'])
            if "time" in data_info:
                collection.dates = data_info['time']
            if "extent" in data_info:
                collection.extent = data_info['extent']
        else:
            collection.bands = ['not specified']
            collection.dates = ['not specified']
            collection.extent = ['not specified']
        return collection

    def image(self, image_product_id) -> 'ImageCollection':
        """
        Get imagery by id.
        :param image_collection_id: String image collection identifier
        :return: collection: RestImagery the imagery with the id
        """
        from .imagery import RestImagery

        image = RestImagery({'product_id': image_product_id}, self)

        # read and format extent, band and date availability information
        data_info = self.get_collection(image_product_id)
        image.bands = []
        if data_info:
            for band in data_info['bands']: image.bands.append(band['band_id'])
            image.dates = data_info['time']
            image.extent = data_info['extent']
        else:
            image.bands = ['not specified']
            image.dates = ['not specified']
            image.extent = ['not specified']
        return image

    def point_timeseries(self, graph, x, y, srs):
        """Compute a timeseries for a given point location."""
        return self.post(self.root + "/timeseries/point?x={}&y={}&srs={}".format(x,y,srs),graph)

    def tiled_viewing_service(self,graph):
        return self.parse_json_response(self.post(self.root + "/tile_service",graph))

    def queue_job(self, job_id):
        """
        Queue the job with a specific id.
        :param job_id: String job identifier
        :return: status_code: Integer Rest Response status code
        """
        request = self.post("/jobs/{}/results".format(job_id), postdata=None)

        return request.status_code

    def job_info(self, job_id):
        # TODO: Maybe add a JobStatus class.
        """
        Get full information about a specific job.
        :param job_id: String job identifier
        :return: status: Dict Info JSON of the job
        """
        request = self.get("/jobs/{}".format(job_id))
        return self.parse_json_response(request)

    def job_results(self, job_id):
        response = self.get("/jobs/{}/results".format(job_id))
        return self.parse_json_response(response)

    def user_download_file(self, file_path, output_file):
        """
        Downloads a user file to the back end.
        :param file_path: remote path to the file that should be downloaded.
        :param output_file: local path, where the file should be saved.
        :return: status: True if it was successful, False otherwise
        """

        path = "/users/{}/files/{}".format(self.userid, file_path)

        resp = self.get(path, stream=True)

        if resp.status_code == 200:
            with open(output_file, 'wb') as f:
                shutil.copyfileobj(resp.raw, f)
            return True
        else:
            return False

    def user_upload_file(self, file_path, remote_path=None):
        """
        Uploads a user file to the back end.
        :param file_path: Local path to the file that should be uploaded.
        :param remote_path: Remote path of the file where it should be uploaded.
        :return: status: True if it was successful, False otherwise
        """
        if not os.path.isfile(file_path):
            return False

        if not remote_path:

            remote_path = os.path.basename(file_path)

        with open(file_path, 'rb') as f:
            input_file = f.read()

        path = "/users/{}/files/{}".format(self.userid, remote_path)

        content_type = {'Content-Type': 'application/octet-stream'}

        resp = self.put(path=path, header=content_type, data=input_file)

        if resp.status_code == 200:
            return True
        else:
            return False

    def user_delete_file(self, file_path):
        """
        Deletes a user file in the back end.

        :param file_path: remote path to the file that should be deleted.
        :return: status: True if it was successful, False otherwise
        """

        path = "/users/{}/files/{}".format(self.userid, file_path)

        resp = self.delete(path)

        if resp.status_code == 200:
            return True
        else:
            return False

    def user_list_files(self):
        """
        Lists all files that the logged in user uploaded.
        :return: file_list: List of the user uploaded files.
        """
        files = self.get('/users/{}/files'.format(self.userid))
        return self.parse_json_response(files)

    # TODO: Maybe rename to execute and merge with execute().
    # Depricated function, use download_job instead.
    def download(self, graph, time, outputfile, format_options):
        """
        Downloads a result of a process graph synchronously.
        :param graph: Dict representing a process graph
        :param time: dba
        :param outputfile: output file
        :param format_options: formating options
        :return: job_id: String
        """
        download_url = self.endpoint + self.root + "/execute"
        request = {
            "process_graph": graph,
            "output": format_options
        }
        r = requests.post(download_url, json=request, stream = True, timeout=1000 )
        if r.status_code == 200:
            with open(outputfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        else:
            raise IOError("Received an exception from the server for url: {} and POST message: {}".format(download_url,json.dumps( request ) ) + r.text)

        return

    def download_job(self, job_id, outputfile, outputformat=None):
        """
        Download an executed job.
        :param job_id: String job identifier
        :param outputfile: String destination path of the resulting file.
        :param outputformat: String format of the resulting file
        :return: status: Dict Status JSON of the resulting job
        """
        if outputformat:
            download_url = "/jobs/{}/download?format={}".format(job_id,outputformat)
        else:
            download_url = "/jobs/{}/download".format(job_id)
        r = self.get(download_url, stream = True)

        if r.status_code == 200:

            url = r.json()
            download_url = url[0]

            auth_header = self.authent.get_header()

            with open(outputfile, 'wb') as handle:
                response = requests.get(download_url, stream=True, headers=auth_header)

                if not response.ok:
                    print (response)

                for block in response.iter_content(1024):

                    if not block:
                        break

                    handle.write(block)
        else:
            raise ConnectionAbortedError(r.text)
        return r.status_code

    def execute(self, graph):
        """
        Execute a process graph synchronously.
        :param graph: Dict representing a process graph
        :return: job_id: String
        """
        response = self.post(self.root + "/execute", graph)
        return self.parse_json_response(response)

    def job(self, graph):
        """
        Submits a new job to the back-end.
        :param graph: Dict representing a process graph
        :return: job_id: String
        """
        response = self.post(self.root + "/jobs", graph)

        job_url = response.headers['Location']
        job_id = urlparse(job_url).path.split('/')[-1]

        return job_id

    def parse_json_response(self, response: requests.Response):
        """
        Parses json response, if an error occurs it raises an Exception.
        :param response: Response of a RESTful request
        :return: response: JSON Response
        """
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 502:
            from requests.exceptions import ProxyError
            return ProxyError("The proxy returned an error, this could be due to a timeout.")
        else:
            raise ConnectionAbortedError(response.text)

    def post(self, path, postdata):
        """
        Makes a RESTful POST request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param postdata: Data of the post request
        :return: response: Response
        """

        auth_header = self.authent.get_header()
        return requests.post(self.endpoint+path, json=postdata, headers=auth_header)

    def patch(self, path):
        """
        Makes a RESTful PATCH request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :return: response: Response
        """
        auth_header = self.authent.get_header()

        return requests.patch(self.endpoint+path, headers=auth_header)

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

        if data:
            return requests.put(self.endpoint+path, headers=head, data=data)
        else:
            return requests.put(self.endpoint+path, headers=head)

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
        else:
            auth_header = {}

        return requests.get(self.endpoint+path, headers=auth_header, stream=stream)

    def delete(self, path):
        """
        Makes a RESTful DELETE request to the backend

        :param path: URL of the request relative to endpoint url
        """

        auth_header = self.authent.get_header()

        return requests.delete(self.endpoint+path, headers=auth_header)

def session(userid=None,endpoint:str="https://openeo.org/openeo"):
    """
    This method is the entry point to OpenEO. You typically create one session object in your script or application, per back-end.
    and re-use it for all calls to that backend.
    If the backend requires authentication, you should set pass your credentials.

    :param endpoint: The http url of an OpenEO endpoint.

    :rtype: openeo.sessions.Session
    """

    return RESTSession(userid,endpoint)

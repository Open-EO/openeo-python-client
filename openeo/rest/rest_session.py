import datetime
import shutil

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
        """
        Loads all jobs of the current user.
        :return: jobs: Dict All jobs of the user
        """
        jobs = self.get(self.root + '/users/{}/jobs'.format(self.userid))
        jobs = json.loads(jobs.text)
        return jobs

    def imagecollections(self) -> dict:
        """
        Loads all available imagecollections types.
        :return: data_dict: Dict All available data types
        """
        data = self.get(self.root + '/data')
        data_dict = json.loads(data.text)
        return data_dict

    def get_collection(self, col_id) -> dict:
        # TODO: Maybe create some kind of Data class.
        """
        Loads detailed information of a specific image collection.
        :param col_id: String Id of the collection
        :return: data_dict: Dict Detailed information about the collection
        """
        if col_id:
            data_info = self.get(self.root + '/data/{}'.format(col_id))
            data_dict = json.loads(data_info.text)
        else:
            data_dict = None

        return data_dict

    def get_all_processes(self) -> dict:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        """
        Loads all available processes of the back end.
        :return: processes_dict: Dict All available processes of the back end.
        """
        processes = self.get('/processes')
        processes_dict = json.loads(processes.text)
        return processes_dict

    def get_process(self, process_id) -> dict:
        # TODO: Maybe create some kind of Process class.
        """
        Get detailed information about a specifig process.
        :param process_id: String Process identifier
        :return: processes_dict: Dict with the detail information about the
                                 process
        """
        if process_id:
            process_info = self.get('/processes/{}'.format(process_id))
            processes_dict = json.loads(process_info.text)
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
        from .imagecollection import RestImageCollection
        collection = RestImageCollection({'collection_id': image_collection_id}, self)
        #TODO session should be used to retrieve collection metadata (containing bands)
        collection.bands = ["B0","B1","B2","B3"]
        collection.dates = [datetime.datetime.now()]
        return collection

    def image(self, image_product_id) -> 'ImageCollection':
        """
        Get imagery by id.
        :param image_collection_id: String image collection identifier
        :return: collection: RestImagery the imagery with the id
        """
        from .imagery import RestImagery

        image = RestImagery({'product_id': image_product_id}, self)
        #TODO session should be used to retrieve collection metadata (containing bands)
        image.bands = ["B0","B1","B2","B3"]
        image.dates = [datetime.datetime.now()]
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
        request = self.patch("/jobs/{}/queue".format(job_id))

        return request.status_code

    def job_status(self, job_id):
        # TODO: Maybe add a JobStatus class.
        """
        Get status of a specific job.
        :param job_id: String job identifier
        :return: status: Dict Status JSON of the job
        """
        request = self.get("/jobs/{}".format(job_id))

        if request.status_code == 200:
            status = json.loads(request.content)
        else:
            return None

        return status

    # TODO: Maybe rename to execute and merge with execute().
    def download(self, graph, time, outputfile,format_options):
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

            url = json.loads(r.text)

            auth_header = self.authent.get_header()
            resp = requests.get(url[0], files={'name': outputfile}, timeout=60,
                                headers=auth_header, stream=True)

            open(outputfile, 'wb').write(resp.content)
        else:
            raise ConnectionAbortedError(r.text)
        return r.status_code
    # TODO: Merge with download().
    def execute(self, graph):
        """
        Execute a process graph synchronously.
        :param graph: Dict representing a process graph
        :return: job_id: String
        """
        response = self.post(self.root + "/execute", graph)
        return self.parse_json_response(response).get("job_id","")

    def job(self, graph):
        """
        Submits a new job to the back-end.
        :param graph: Dict representing a process graph
        :return: job_id: String
        """
        response = self.post(self.root + "/jobs", graph)
        return self.parse_json_response(response).get("job_id","")

    def parse_json_response(self, response: requests.Response):
        """
        Parses json response, if an error occurs it raises an Exception.
        :param response: Response of a RESTful request
        :return: response: JSON Response
        """
        if response.status_code == 200:
            return response.json()
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

    def get(self,path, stream=False):
        """
        Makes a RESTful GET request to the back end.
        :param path: URL of the request (without root URL e.g. "/data")
        :param stream: True if the get request should be streamed, else False
        :return: response: Response
        """
        auth_header = self.authent.get_header()

        return requests.get(self.endpoint+path, headers=auth_header, stream=stream)


def session(userid=None,endpoint:str="https://openeo.org/openeo"):
    """
    This method is the entry point to OpenEO. You typically create one session object in your script or application, per back-end.
    and re-use it for all calls to that backend.
    If the backend requires authentication, you should set pass your credentials.

    :param endpoint: The http url of an OpenEO endpoint.

    :rtype: openeo.sessions.Session
    """

    return RESTSession(userid,endpoint)

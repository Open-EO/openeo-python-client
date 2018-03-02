import datetime
import shutil

import json

import requests
from requests.auth import HTTPBasicAuth

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
        self.userid = userid
        self.endpoint = endpoint
        self.root = "/openeo"
        self.token = None

    #@property
    #@abstractmethod
    def auth(self, username, password) -> str:
        #TODO: Create some kind of Authentication class for different authentication strategies of the endpoints.
        token = requests.post(self.endpoint+'/auth/login', auth=HTTPBasicAuth('test', 'test'))

        if token.status_code == 200:
            self.token = json.loads(token.text)["token"]

        return self.token

    def user_jobs(self) -> dict:
        #TODO: Create a kind of User class to abstract the information (e.g. userid, username, password from the session.
        jobs = self.get('/users/{}/jobs'.format(self.userid))
        jobs = json.loads(jobs.text)
        return jobs

    def get_all_data(self) -> dict:
        # TODO: Same as get_all_process.
        data = self.get('/data/')
        data_dict = json.loads(data.text)
        return data_dict

    def get_data(self, data_id) -> dict:
        # TODO: Maybe create some kind of Data class.
        if data_id:
            data_info = self.get('/data/{}'.format(data_id))
            data_dict = json.loads(data_info.text)
        else:
            data_dict = None

        return data_dict

    def get_all_processes(self) -> dict:
        # TODO: Maybe format the result dictionary so that the process_id is the key of the dictionary.
        processes = self.get('/processes/')
        processes_dict = json.loads(processes.text)
        return processes_dict

    def get_process(self, process_id) -> dict:
        # TODO: Maybe create some kind of Process class.
        if process_id:
            process_info = self.get('/processes/{}'.format(process_id))
            processes_dict = json.loads(process_info.text)
        else:
            processes_dict = None

        return processes_dict

    def create_job(self, post_data, evaluation="lazy") -> str:
        # TODO: Create a Job class or something for the creation of a nested process execution...

        job_status = self.post("/jobs/?evaluate={}".format(evaluation), post_data)
        print(str(job_status.text))
        if job_status.status_code == 200:
            job_info = json.loads(job_status.text)
            if 'job_id' in job_info:
                job_id = job_info['job_id']
        else:
            job_id = None

        return job_id

    def imagecollection(self, image_collection_id) -> 'ImageCollection':
        from .imagecollection import RestImageCollection
        collection = RestImageCollection({'collection_id': image_collection_id}, self)
        #TODO session should be used to retrieve collection metadata (containing bands)
        collection.bands = ["B0","B1","B2"]
        collection.dates = [datetime.datetime.now()]
        return collection

    def point_timeseries(self, graph, x, y, srs):
        """Compute a timeseries for a given point location."""
        return self.post(self.root + "/timeseries/point?x={}&y={}&srs={}".format(x,y,srs),graph)

    def tiled_viewing_service(self,graph):
        return self.parse_json_response(self.post(self.root + "/tile_service",graph))

    def download(self, graph, time, outputfile,format_options):

        download_url = self.endpoint + self.root + "/execute"
        request = {
            "process_graph":graph,
            "output":format_options
        }
        r = requests.post(download_url, json=request, stream = True, timeout=1000 )
        if r.status_code == 200:
            with open(outputfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        else:
            raise IOError("Received an exception from the server for url: {} and POST message: {}".format(download_url,json.dumps( graph ) ) + r.text)


        return

    def download_job(self, job_id, outputfile,outputformat):
        download_url = self.endpoint + self.root + "/jobs/{}/download?format={}".format( job_id,outputformat)
        r = requests.get(download_url, stream = True)
        if r.status_code == 200:
            with open(outputfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        else:
            raise ConnectionAbortedError(r.text)

        return

    def download_image(self, job_id, outputfile,outputformat):
        #download_url = self.endpoint + "/download/{}?format={}".format( job_id,outputformat)
        r = self.get("/download/{}?format={}".format( job_id,outputformat), stream=True)
        if r.status_code == 200:
            with open(outputfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        else:
            raise ConnectionAbortedError(r.text)

        return

    def execute(self,graph):
        """
        Execute a process graph synchronously.
        :param graph: Dict representing a process graph
        :return:
        """
        response = self.post(self.root + "/execute", graph)
        return self.parse_json_response(response).get("job_id","")

    def job(self,graph,batch=False):
        response = self.post(self.root + "/jobs", graph)
        return self.parse_json_response(response).get("job_id","")

    def parse_json_response(self,response:requests.Response):
        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionAbortedError(response.text)


    def post(self,path,postdata):
        if self.token:
            return requests.post(self.endpoint+path, json=postdata, headers={'Authorization': 'Bearer {}'.format(self.token)})
        else:
            return requests.post(self.endpoint + path, json=postdata)

    def get(self,path, stream=False):
        if self.token:
            wholepath = self.endpoint + path
            return requests.get(self.endpoint+path, headers={'Authorization': 'Bearer {}'.format(self.token)}, stream=stream)
        else:
            return requests.get(self.endpoint + path, stream=stream)


def session(userid=None,endpoint:str="https://openeo.org/openeo"):
    """
    Returns a :class:`Session` for context-management.

    :rtype: Session
    """

    return RESTSession(userid,endpoint)
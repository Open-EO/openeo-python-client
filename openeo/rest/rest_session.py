import datetime
import shutil

import json

import requests
from requests.auth import HTTPBasicAuth

from ..sessions import Session

"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Session object to manage and persist settings when interacting with the OpenEO API.
"""


class RESTSession(Session):

    def __init__(self,userid, endpoint):
        self.userid = userid
        self.endpoint = endpoint
        self.root = "/openeo"
        self.token = None

    #@property
    #@abstractmethod
    def auth(self, username, password) -> str:
        token = requests.post(self.endpoint+'/auth/login', auth=HTTPBasicAuth('test', 'test'))

        if token.status_code == 200:
            self.token = json.loads(token.text)["token"]

        return self.token

    def user_jobs(self):
        jobs = self.get('/users/{}/jobs'.format(self.userid))
        jobs = json.loads(jobs.text)
        return jobs

    def get_all_data(self):
        data = self.get('/data/')
        data_dict = json.loads(data.text)
        return data_dict

    def get_data(self, data_id):
        if data_id:
            data_info = self.get('/data/{}'.format(data_id))
            data_dict = json.loads(data_info.text)
        else:
            data_dict = None

        return data_dict

    def get_all_processes(self):
        processes = self.get('/processes/')
        processes_dict = json.loads(processes.text)
        return processes_dict

    def get_process(self, process_id):
        if process_id:
            process_info = self.get('/processes/{}'.format(process_id))
            processes_dict = json.loads(process_info.text)
        else:
            processes_dict = None

        return processes_dict

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
        return self.post(self.root + "/tile_service",graph)

    def download(self, graph, time, outputformat, outputfile):
        with open(outputfile, 'wb') as f:
            download_url = self.endpoint + self.root + "/download?date={}&outputformat={}".format(time, outputformat)
            r = requests.post(download_url, json=graph, stream = True)
            shutil.copyfileobj(r.raw, f)

        return

    def download_job(self, job_id, outputfile,outputformat):
        download_url = self.endpoint + self.root + "/download/wcs/{}?outputformat={}".format( job_id,outputformat)
        r = requests.get(download_url, stream = True)
        if r.status_code == 200:
            with open(outputfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        else:
            raise ConnectionAbortedError(r.text)

        return


    def job(self,graph,batch=False) -> str:
        response = self.post(self.root + "/jobs".format(batch), graph)
        return self.parse_json_response(response).get("job_id","")

    def parse_json_response(self,response:requests.Response):
        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionAbortedError(response.text)


    def post(self,path,postdata):
        if self.token:
            return requests.post(self.endpoint+path,json=postdata, headers={'Authorization': 'Bearer {}'.format(self.token)})
        else:
            return requests.post(self.endpoint + path, json=postdata)

    def get(self,path):
        if self.token:
            return requests.get(self.endpoint+path, headers={'Authorization': 'Bearer {}'.format(self.token)})
        else:
            return requests.get(self.endpoint + path)


def session(userid=None,endpoint:str="https://openeo.org/openeo"):
    """
    Returns a :class:`Session` for context-management.

    :rtype: Session
    """

    return RESTSession(userid,endpoint)
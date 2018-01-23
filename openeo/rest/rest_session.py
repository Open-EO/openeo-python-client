import datetime
import shutil

import requests

from ..sessions import Session

"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Session object to manage and persist settings when interacting with the OpenEO API.
"""


class RESTSession(Session):

    def __init__(self,username, endpoint):
        self.username = username
        self.endpoint = endpoint
        self.root = "/openeo"

    @property
    #@abstractmethod
    def auth(self) -> str:
        pass

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
            download_url = self.endpoint + self.root + "/download".format(time, outputformat)
            r = requests.post(download_url, json=graph, stream = True, timeout=1000 )
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
        return requests.post(self.endpoint+path,json=postdata)




def session(username=None,endpoint:str="https://openeo.org/openeo"):
    """
    Returns a :class:`Session` for context-management.

    :rtype: Session
    """

    return RESTSession(username,endpoint)
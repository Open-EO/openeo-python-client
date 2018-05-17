import base64
from typing import List, Dict, Union

import cloudpickle
from datetime import datetime, date
from pandas import Series
import pandas as pd

from openeo.job import Job
from openeo.rest.job import ClientJob
from ..imagecollection import ImageCollection
from ..sessions import Session
from shapely.geometry import Polygon, MultiPolygon, mapping


class RestImageCollection(ImageCollection):
    """Class representing an Image Collection. """


    def __init__(self, parentgraph:Dict,session:Session):
        self.graph = parentgraph
        self.session = session

    def date_range_filter(self, start_date: Union[str, datetime, date],
                          end_date: Union[str, datetime, date]) -> 'ImageCollection':
        graph = {
            'process_id': 'filter_daterange',
            'args' : {
                'imagery': self.graph,
                'from':pd.to_datetime( start_date ).isoformat(),
                'to': pd.to_datetime( end_date ) .isoformat()
            }
        }
        return RestImageCollection(graph,session=self.session)

    def bbox_filter(self, left, right, top, bottom, srs) -> 'ImageCollection':
        graph = {
            'process_id': 'filter_bbox',
            'args' : {
                'collections':[self.graph],
                'left':left,
                'right': right,
                'top':top,
                'bottom':bottom,
                'srs':srs
            }
        }
        return RestImageCollection(graph,session=self.session)

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        pickled_lambda = cloudpickle.dumps(bandfunction)
        graph = {
            'process_id': 'apply_pixel',
            'args' : {
                'collections':[self.graph],
                'bands':bands,
                'function': str(base64.b64encode(pickled_lambda),"UTF-8")
            }
        }
        return RestImageCollection(graph,session=self.session)

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        """ Applies a windowed reduction to a timeseries by applying a user defined function.

            :param temporal_window: The time window to group by
            :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.
            :return A pandas Timeseries object
        """
        # /api/jobs
        pickled_lambda = cloudpickle.dumps(aggregationfunction)
        graph = {
            'process_id': 'reduce_by_time',
            'args' : {
                'collections':[self.graph],
                'temporal_window': temporal_window,
                'function': str(base64.b64encode(pickled_lambda),"UTF-8")
            }
        }
        return RestImageCollection(graph,session=self.session)

    def min_time(self) -> 'ImageCollection':
        graph = {
            'process_id': 'min_time',
            'args' : {
                'collections':[self.graph]
            }
        }
        return RestImageCollection(graph,session=self.session)

    def max_time(self) -> 'ImageCollection':
        graph = {
            'process_id': 'max_time',
            'args' : {
                'collections':[self.graph]
            }
        }
        return RestImageCollection(graph,session=self.session)


    ####VIEW methods #######
    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        """
        Extract a time series for the given point location.

        :param x: The x coordinate of the point
        :param y: The y coordinate of the point
        :param srs: The spatial reference system of the coordinates, by default this is 'EPSG:4326', where x=longitude and y=latitude.
        :return: Dict: A timeseries
        """
        return self.session.point_timeseries({"process_graph":self.graph}, x, y, srs)

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon]) -> 'ImageCollection':
        """
        Extract a mean time series for the given (multi)polygon. Its points are expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon
        :param srs: The spatial reference system of the coordinates, by default this is 'EPSG:4326'
        :return: ImageCollection
        """

        geojson = mapping(polygon)
        geojson['crs'] = {
            'type': 'name',
            'properties': {
                'name': 'EPSG:4326'
            }
        }

        graph = {
            'process_id': 'zonal_statistics',
            'args': {
                'imagery': self.graph,
                'geometry': geojson
            }
        }

        return RestImageCollection(graph, self.session)

    def download(self,outputfile:str, bbox="", time="",**format_options) -> str:
        """Extraxts a geotiff from this image collection."""
        return self.session.download({"process_graph":self.graph},time,outputfile,format_options)

    def tiled_viewing_service(self) -> Dict:
        return self.session.tiled_viewing_service({"process_graph":self.graph})

    def send_job(self) -> Job:
        return ClientJob(self.session.job({"process_graph":self.graph}),self.session)

    def execute(self) -> Dict:
        return self.session.execute({"process_graph":self.graph})

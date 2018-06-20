import base64
from typing import List, Dict, Union

import cloudpickle
from datetime import datetime, date
from pandas import Series

from openeo.job import Job
from openeo.rest.job import ClientJob
from ..imagecollection import ImageCollection
from ..sessions import Session
from shapely.geometry import Polygon, MultiPolygon, mapping

# Created for the use of EODC PoC
# Same as collection, but uses "imagery" instead of "collections"
#TODO: remove imagecollection and replace it with this (maybe rename this to Imagery)
class RestImagery(ImageCollection):
    """Class representing an Image Collection. """


    def __init__(self, parentgraph:Dict,session:Session):
        self.graph = parentgraph
        self.session = session

    def date_range_filter(self, start_date: Union[str, datetime, date],
                          end_date: Union[str, datetime, date]) -> 'ImageCollection':
        graph = {
            'process_id': 'filter_daterange',
            'args' : {
                'imagery':self.graph,
                'from':start_date,
                'to': end_date
            }
        }
        return RestImagery(graph, session=self.session)

    def bbox_filter(self, left, right, top, bottom, srs) -> 'ImageCollection':
        graph = {
            'process_id': 'filter_bbox',
            'args': {
                'imagery':self.graph,
                'left': left,
                'right': right,
                'top': top,
                'bottom': bottom,
                'srs': srs
            }
        }
        return RestImagery(graph, session=self.session)

    def band_filter(self, bands):
        """Filter the imagery by the given bands
            :param bands: List of band names or single band name as a string.
        """

        graph = {
            'process_id': 'filter_bands',
            'args': {
                'imagery': self.graph,
                'bands': bands
            }
        }
        return RestImagery(graph, session=self.session)

    def zonal_statistics(self, regions, func, scale=1000, interval="day"):
        graph = {
            'process_id': 'zonal_statistics',
            'args': {
                'imagery': self.graph,
                'regions': regions,
                'func': func,
                'scale': scale,
                'interval': interval
            }
        }
        return RestImagery(graph, session=self.session)

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        pickled_lambda = cloudpickle.dumps(bandfunction)
        graph = {
            'process_id': 'apply_pixel',
            'args' : {
                'imagery':self.graph,
                'bands':bands,
                'function': str(base64.b64encode(pickled_lambda), "UTF-8")
            }
        }
        return RestImagery(graph, session=self.session)

    def apply_tiles(self, code: str) -> 'ImageCollection':
        """Apply a function to the given set of tiles in this image collection.
            Code should follow the OpenEO UDF conventions.
            :param code: String representing Python code to be executed in the backend.
        """
        graph = {
            'process_id': 'apply_tiles',
            'args' : {
                'imagery':self.graph,
                'code':{
                    'language':'python',
                    'source':code
                }
            }
        }
        return RestImagery(graph, session=self.session)

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
            'args': {
                'imagery':self.graph,
                'temporal_window': temporal_window,
                'function': str(base64.b64encode(pickled_lambda), "UTF-8")
            }
        }
        return RestImagery(graph, session=self.session)

    def min_time(self) -> 'ImageCollection':
        graph = {
            'process_id': 'min_time',
            'args': {
                'imagery': self.graph
            }
        }
        return RestImagery(graph, session=self.session)

    def max_time(self) -> 'ImageCollection':
        graph = {
            'process_id': 'max_time',
            'args': {
                'imagery': self.graph
            }
        }
        return RestImagery(graph, session=self.session)

    def ndvi(self, red, nir) -> 'ImageCollection':
        graph = {
            'process_id': 'NDVI',
            'args': {
                'imagery': self.graph,
                'red': red,
                'nir': nir
            }
        }
        return RestImagery(graph, session=self.session)

    def stretch_colors(self, min, max) -> 'ImageCollection':
        graph = {
            'process_id': 'stretch_colors',
            'args': {
                'imagery': self.graph,
                'imagery': self.graph,
                'min': min,
                'max': max
            }
        }
        return RestImagery(graph, session=self.session)

    ####VIEW methods #######
    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        """
        Extract a time series for the given point location.

        :param x: The x coordinate of the point
        :param y: The y coordinate of the point
        :param srs: The spatial reference system of the coordinates, by default
        this is 'EPSG:4326', where x=longitude and y=latitude.
        :return: Dict: A timeseries
        """
        return self.session.point_timeseries({"process_graph":self.graph}, x, y, srs)

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon]) -> 'ImageCollection':
        """
        Extract a mean time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon
        :param srs: The spatial reference system of the coordinates, by default
        this is 'EPSG:4326'
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

        return RestImagery(graph, self.session)

    def download(self, outputfile:str, bbox="", time="", **format_options) -> str:
        """Extraxts a geotiff from this image collection."""
        return self.session.download(self.graph, time, outputfile, format_options)

    def tiled_viewing_service(self) -> Dict:
        return self.session.tiled_viewing_service({"process_graph":self.graph})

    def send_job(self, out_format=None) -> Job:
        """
        Sends a job to the backend and returns a ClientJob instance.
        :param out_format: String Format of the job result.
        :return: status: ClientJob resulting job.
        """
        if out_format:
            return ClientJob(self.session.job({"process_graph": self.graph,
                                               'output': {'format': out_format}}), self.session)
        else:
            return ClientJob(self.session.job({"process_graph": self.graph}), self.session)

    def execute(self) -> Dict:
        """Executes the process graph of the imagery. """
        return self.session.execute({"process_graph": self.graph})

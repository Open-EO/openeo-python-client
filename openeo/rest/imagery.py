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


class RestImagery(ImageCollection):
    """Class representing an Image Collection. (In the API as 'imagery')"""

    def __init__(self, parentgraph:Dict,session:Session):
        self.graph = parentgraph
        self.session = session

    def date_range_filter(self, start_date: Union[str, datetime, date],
                          end_date: Union[str, datetime, date]) -> 'ImageCollection':
        """Drops observations from a collection that have been captured before
            a start or after a given end date.
            :param start_date: starting date of the filter
            :param end_date: ending date of the filter
            :return An ImageCollection instance
        """
        process_id = 'filter_daterange'
        args = {
                'imagery': self.graph,
                'from': start_date,
                'to': end_date
            }

        return self.graph_add_process(process_id, args)

    def bbox_filter(self, left, right, top, bottom, srs) -> 'ImageCollection':
        """Drops observations from a collection that are located outside
            of a given bounding box.
            :param left: left boundary (longitude / easting)
            :param right: right boundary (longitude / easting)
            :param top: top boundary (latitude / northing)
            :param bottom: top boundary (latitude / northing)
            :param srs: spatial reference system of boundaries as
                        proj4 or EPSG:12345 like string
            :return An ImageCollection instance
        """
        process_id = 'filter_bbox'
        args = {
                'imagery': self.graph,
                'left': left,
                'right': right,
                'top': top,
                'bottom': bottom,
                'srs': srs
            }
        return self.graph_add_process(process_id, args)

    def band_filter(self, bands) -> 'ImageCollection':
        """Filter the imagery by the given bands
            :param bands: List of band names or single band name as a string.
            :return An ImageCollection instance
        """

        process_id = 'filter_bands'
        args = {
                'imagery': self.graph,
                'bands': bands
                }
        return self.graph_add_process(process_id, args)

    def zonal_statistics(self, regions, func, scale=1000, interval="day") -> 'ImageCollection':
        """Calculates statistics for each zone specified in a file.
            :param regions: GeoJSON or a path to a GeoJSON file containing the
                            regions. For paths you must specify the path to a
                            user-uploaded file without the user id in the path.
            :param func: Statistical function to calculate for the specified
                         zones. example values: min, max, mean, median, mode
            :param scale: A nominal scale in meters of the projection to work
                          in. Defaults to 1000.
            :param interval: Interval to group the time series. Allowed values:
                            day, wee, month, year. Defaults to day.
            :return An ImageCollection instance
        """
        regions_geojson = regions
        if isinstance(regions,Polygon) or isinstance(regions,MultiPolygon):
            regions_geojson = mapping(regions)
        process_id = 'zonal_statistics'
        args = {
                'imagery': self.graph,
                'regions': regions_geojson,
                'func': func,
                'scale': scale,
                'interval': interval
            }

        return self.graph_add_process(process_id, args)

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        pickled_lambda = cloudpickle.dumps(bandfunction)

        process_id = 'apply_pixel'
        args = {
                'imagery':self.graph,
                'bands':bands,
                'function': str(base64.b64encode(pickled_lambda), "UTF-8")
            }

        return self.graph_add_process(process_id, args)

    def apply_tiles(self, code: str) -> 'ImageCollection':
        """Apply a function to the given set of tiles in this image collection.
            Code should follow the OpenEO UDF conventions.
            :param code: String representing Python code to be executed in the backend.
        """

        process_id = 'apply_tiles'
        args = {
                'imagery':self.graph,
                'code':{
                    'language':'python',
                    'source':code
                }
            }

        return self.graph_add_process(process_id, args)

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        """ Applies a windowed reduction to a timeseries by applying a user
            defined function.
            :param temporal_window: The time window to group by
            :param aggregationfunction: The function to apply to each time window.
                                        Takes a pandas Timeseries as input.
            :return A pandas Timeseries object
        """
        pickled_lambda = cloudpickle.dumps(aggregationfunction)

        process_id = 'reduce_by_time'
        args = {
                'imagery':self.graph,
                'temporal_window': temporal_window,
                'function': str(base64.b64encode(pickled_lambda), "UTF-8")
            }

        return self.graph_add_process(process_id, args)

    def min_time(self) -> 'ImageCollection':
        """Finds the minimum value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """

        process_id = 'min_time'
        args = {
                'imagery': self.graph
                }

        return self.graph_add_process(process_id, args)

    def max_time(self) -> 'ImageCollection':
        """Finds the maximum value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """

        process_id = 'max_time'

        args = {
                'imagery': self.graph
            }

        return self.graph_add_process(process_id, args)

    def mean_time(self) -> 'ImageCollection':
        """Finds the mean value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """

        process_id = 'mean_time'

        args = {
                'imagery': self.graph
            }

        return self.graph_add_process(process_id, args)

    def median_time(self) -> 'ImageCollection':
        """Finds the median value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """

        process_id = 'median_time'

        args = {
                'imagery': self.graph
            }

        return self.graph_add_process(process_id, args)

    def ndvi(self, red, nir) -> 'ImageCollection':
        """ NDVI
            :param red: Reference to the red band
            :param nir: Reference to the nir band
            :return An ImageCollection instance
        """
        process_id = 'NDVI'

        args = {
                'imagery': self.graph,
                'red': red,
                'nir': nir
            }

        return self.graph_add_process(process_id, args)

    def stretch_colors(self, min, max) -> 'ImageCollection':
        """ Color stretching
            :param min: Minimum value
            :param max: Maximum value
            :return An ImageCollection instance
        """
        process_id = 'stretch_colors'
        args = {
                'imagery': self.graph,
                'min': min,
                'max': max
            }

        return self.graph_add_process(process_id, args)

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

        process_id = 'zonal_statistics'

        args = {
                'imagery': self.graph,
                'regions': geojson,
                'func': 'avg'
            }

        return self.graph_add_process(process_id, args)

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

    ####### HELPER methods #######

    def graph_add_process(self, process_id, args) -> 'ImageCollection':
        """
        Returns a new restimagery with an added process with the given process
        id and a dictionary of arguments
        :param process_id: String, Process Id of the added process.
        :param args: Dict, Arguments of the process.
        :return: imagery: Instance of the RestImagery class
        """
        graph = {
            'process_id': process_id,
            'args': args
        }

        return RestImagery(graph, self.session)

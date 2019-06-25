import base64
from typing import List, Dict, Union

import cloudpickle
from datetime import datetime, date
from pandas import Series

from openeo.job import Job
from openeo.rest.job import RESTJob
from openeo.imagecollection import ImageCollection
from openeo.connection import Connection
from openeo.rest.rest_processgraph import RESTProcessgraph
from shapely.geometry import Polygon, MultiPolygon, mapping


class RESTProcesses():
    """Class representing the Processes.
        EXPERIMENTAL, only supports OpenEO 0.3

    """

    def __init__(self, connection:Connection):
        self.connection = connection

    def get_collection(self, name) -> 'RESTProcessgraph':
        """
        Get imagery by id.
        :param name: String image collection identifier
        :return: process graph: RestProcessGraph the imagery with the id
        """

        pgraph = RESTProcessgraph(pg_id=None, connection=self.connection)

        pgraph.graph = {"process_id": "get_collection", "name": name}

        return pgraph

    def filter_daterange(self, imagery, extent) -> 'ProcessGraph':
        """Drops observations from a collection that have been captured before
            a start or after a given end date.
            :param imagery: eodata object (ProcessGraph)
            :param extent: List of starting date and ending date of the filter
            :return An ProcessGraph instance
        """

        graph = {
                'process_id': 'filter_daterange',
                'imagery': imagery.graph,
                'extent': extent
            }

        imagery.graph = graph

        return imagery

    def filter_bbox(self, imagery, west, east, north, south, crs=None, base=None, height=None) -> 'ImageCollection':
        """Drops observations from a collection that are located outside
            of a given bounding box.
            :param imagery: eodata object (ProcessGraph)
            :param west: west boundary (longitude / easting)
            :param east: east boundary (longitude / easting)
            :param top: top boundary (latitude / northing)
            :param bottom: top boundary (latitude / northing)
            :param crs: spatial reference system of boundaries as
                        proj4 or EPSG:12345 like string
            :param base: lower left corner coordinate axis 3
            :param height: upper right corner coordinate axis 3
            :return An ProcessGraph instance
        """

        graph = {
                'process_id': 'filter_bbox',
                'imagery': imagery.graph,
                'extent':
                     {
                        'west': west,
                        'east': east,
                        'north': north,
                        'south': south
                     }
            }

        if crs:
            graph['extent']['crs'] = crs
        else:
            graph['extent']['crs'] = "EPSG: 4326"
        if base:
            graph['extent']['base'] = base
        if height:
            graph['extent']['height'] = height

        imagery.graph = graph

        return imagery

    def filter_bands(self, imagery, bands=None, names=None, wavelengths=None) -> 'ImageCollection':
        """Filter the imagery by the given bands
            :param imagery: eodata (Process Graph)
            :param bands: List of band ids as strings.
            :param names: List of band names as strings.
            :param wavelengths: Either a number specifying a specific wavelength or a two-element array of
                                numbers specifying a minimum and maximum wavelength..
            :return An ProcessGraph instance with added band filter
        """

        graph = {
                'process_id': 'filter_bands',
                'imagery': imagery.graph,
                }

        if bands:
            graph['bands'] = bands
        if names:
            graph['names'] = names
        if wavelengths:
            graph['wavelengths'] = wavelengths

        imagery.graph = graph
        return imagery

    def zonal_statistics(self, imagery, regions, func, scale=1000, interval="day") -> 'ImageCollection':
        """Calculates statistics for each zone specified in a file.
            :param imagery: eodata (Process Graph)
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

        graph = {
                'process_id': 'zonal_statistics',
                'imagery': imagery.graph,
                'regions': regions_geojson,
                'func': func,
                'scale': scale,
                'interval': interval
            }

        imagery.graph = graph

        return imagery

    def min_time(self, imagery) -> 'ImageCollection':
        """Finds the minimum value of a time series for all bands of the input dataset.
            :param imagery: eodata (Process Graph)
            :return An ProcessGraph instance
        """

        graph = {
            'process_id': 'min_time',
            'imagery': imagery.graph
        }

        imagery.graph = graph

        return imagery

    def max_time(self, imagery) -> 'ImageCollection':
        """Finds the maximum value of a time series for all bands of the input dataset.
            :param imagery: eodata (Process Graph)
            :return An ProcessGraph instance
        """

        graph = {
            'process_id': 'max_time',
            'imagery': imagery.graph
        }

        imagery.graph = graph

        return imagery

    def ndvi(self, imagery, red, nir) -> 'ImageCollection':
        """ NDVI
            :param imagery: eodata (Process Graph)
            :param red: Reference to the red band
            :param nir: Reference to the nir band
            :return An ImageCollection instance
        """

        graph = {
                'process_id': 'NDVI',
                'imagery': imagery.graph,
                'red': red,
                'nir': nir
            }

        imagery.graph = graph

        return imagery

    def get_results(self, url=None, job_id=None) -> 'ImageCollection':
        """ Filters and selects a single collection provided by the back-end. The back-end provider decides which of
            the potential collections is the most relevant one to be selected.
            :param url: An URL to job results.
            :param job_id: An identifier of a job on the back-end this process is running on.
            :return An ImageCollection instance
        """

        pgraph = RESTProcessgraph(pg_id=None, connection=self.connection)

        graph = {
                'process_id': 'get_results',
            }

        if url:
            graph["url"] = url
        if job_id:
            graph["job_id"] = job_id

        pgraph.graph = graph

        return pgraph

    def process_graph(self, imagery, url, variables=None) -> 'ImageCollection':
        """ Loads another process graph and applies it to the specified imagery.
            This can be an externally hosted process graph.
            :param imagery: An URL to job results.
            :param url: An URL to a process graph.
            :param variables: An object holding key-value-pairs with values for variables that are defined by the
                              process graph. The key of the pair has to be the corresponding variable_id for the value
                              specified. The replacement for the variable is the value of the pair.
            :return An ImageCollection instance
        """

        graph = {
            'process_id': 'process_graph',
            'imagery': imagery.graph,
            'url': url
        }

        if variables:
            graph["variables"] = variables

        imagery.graph = graph

        return imagery

    #   Processes below are not defined in the process reference at ---------------------------------------------------
    #   https://open-eo.github.io/openeo-api/v/0.3.1/processreference/ ------------------------------------------------

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

    def count_time(self) -> 'ImageCollection':
        """Counts the number of images with a valid mask in a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """

        process_id = 'count_time'

        args = {
                'imagery': self.graph
            }

        return self.graph_add_process(process_id, args)

    def stretch_colors(self,imagery, min, max) -> 'ImageCollection':
        """ Color stretching
            :param min: Minimum value
            :param max: Maximum value
            :return An ImageCollection instance
        """
        process_id = 'stretch_colors'
        args = {
                'imagery': imagery.graph,
                'min': min,
                'max': max
            }
        imagery.graph = {
            'process_id':process_id,
            'args':args
        }

        return imagery

    def mask(self, polygon: Union[Polygon, MultiPolygon], srs="EPSG:4326") -> 'ImageCollection':
        """
        Mask the image collection using a polygon. All pixels outside the polygon should be set to the nodata value.
        All pixels inside, or intersecting the polygon should retain their original value.

        :param polygon: A polygon, provided as a Shapely Polygon or MultiPolygon
        :param srs: The reference system of the provided polygon, by default this is Lat Lon (EPSG:4326).
        :return: A new ImageCollection, with the mask applied.
        """
        geojson = mapping(polygon)
        geojson['crs'] = {
            'type': 'name',
            'properties': {
                'name': srs
            }
        }

        process_id = 'mask'

        args = {
            'imagery': self.graph,
            'mask_shape': geojson
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
        return self.connection.point_timeseries({"process_graph":self.graph}, x, y, srs)

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
        return self.connection.download(self.graph, time, outputfile, format_options)

    def tiled_viewing_service(self,**kwargs) -> Dict:
        return self.connection.create_service(self.graph,**kwargs)

    def send_job(self, out_format=None, **format_options) -> Job:
        """
        Sends a job to the backend and returns a ClientJob instance.
        :param out_format: String Format of the job result.
        :param format_options: String Parameters for the job result format
        :return: status: ClientJob resulting job.
        """
        if out_format:
            return RESTJob(self.connection.job({"process_graph": self.graph,
                                               'output': {
                                                   'format': out_format,
                                                   'parameters': format_options
                                               }}), self.connection)
        else:
            return RESTJob(self.connection.job({"process_graph": self.graph}), self.connection)

    def execute(self) -> Dict:
        """Executes the process graph of the imagery. """
        return self.connection.execute({"process_graph": self.graph})

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

        return RESTProcesses(graph, self.connection)

    # def graph_add_process(self, process_id, args) -> 'ImageCollection':
    #     """
    #     Returns a new restimagery with an added process with the given process
    #     id and a dictionary of arguments
    #     :param process_id: String, Process Id of the added process.
    #     :param args: List, Arguments of the process.
    #     :return: Dict: Process Graph dictionary
    #     """
    #
    #
    #     graph = {
    #         'process_id': process_id,
    #     }
    #
    #     for arg in args:
    #         graph[arg['name']] = arg['value']
    #
    #
    #     return graph

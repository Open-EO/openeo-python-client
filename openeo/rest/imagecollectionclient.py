from typing import List, Dict, Union

from datetime import datetime, date

from openeo.job import Job
from openeo.rest.job import RESTJob
from ..imagecollection import ImageCollection
from ..connection import Connection
from shapely.geometry import Polygon, MultiPolygon, mapping
from ..graphbuilder import GraphBuilder


class ImageCollectionClient(ImageCollection):
    """Class representing an Image Collection. (In the API as 'imagery')"""

    def __init__(self,node_id:str, builder:GraphBuilder,session:Connection):
        self.node_id = node_id
        self.builder= builder
        self.session = session
        self.graph = builder.processes

    @classmethod
    def create_collection(cls, collection_id:str,session:Connection = None):
        """
        Create a new Image Collection/Raster Data cube.
        :param collection_id: A collection id, should exist in the backend.
        :param session: The session to use to connect with the backend.
        :return:
        """
        from ..graphbuilder import GraphBuilder
        builder = GraphBuilder()
        id = builder.process("get_collection", {'name': collection_id})
        return ImageCollectionClient(id,builder,session)


    def date_range_filter(self, start_date: Union[str, datetime, date],
                          end_date: Union[str, datetime, date]) -> 'ImageCollection':
        """Drops observations from a collection that have been captured before
            a start or after a given end date.
            :param start_date: starting date of the filter
            :param end_date: ending date of the filter
            :return An ImageCollection instance
        """
        process_id = 'filter_temporal'
        args = {
                'data':{'from_node': self.node_id},
                'from': start_date,
                'to': end_date
            }

        return self.graph_add_process(process_id, args)



    def bbox_filter(self, west=None, east=None, north=None, south=None, crs=None,left=None, right=None, top=None, bottom=None, srs=None) -> 'ImageCollection':
        """Drops observations from a collection that are located outside
            of a given bounding box.

            :param east: east boundary (longitude / easting)
            :param west: west boundary (longitude / easting)
            :param north: north boundary (latitude / northing)
            :param south: south boundary (latitude / northing)
            :param srs: coordinate reference system of boundaries as
                        proj4 or EPSG:12345 like string
            :return An ImageCollection instance
        """
        process_id = 'filter_bbox'
        args = {
                'data': {'from_node': self.node_id},
                'west': west or left,
                'east': east or right,
                'north': north or top,
                'south': south or bottom,
                'crs': crs or srs
            }
        return self.graph_add_process(process_id, args)

    def band_filter(self, bands) -> 'ImageCollection':
        """Filter the imagery by the given bands
            :param bands: List of band names or single band name as a string.
            :return An ImageCollection instance
        """

        process_id = 'filter_bands'
        args = {
                'data': {'from_node': self.node_id},
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
                'data': {'from_node': self.node_id},
                'regions': regions_geojson,
                'func': func,
                'scale': scale,
                'interval': interval
            }

        return self.graph_add_process(process_id, args)

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        raise NotImplementedError("apply_pixel no longer supported")

    def apply_tiles(self, code: str) -> 'ImageCollection':
        """Apply a function to the given set of tiles in this image collection.
            Code should follow the OpenEO UDF conventions.
            :param code: String representing Python code to be executed in the backend.
        """

        process_id = 'apply_tiles'
        args = {
                'data': {'from_node': self.node_id},
                'code':{
                    'language':'python',
                    'source':code
                }
            }

        return self.graph_add_process(process_id, args)

    def apply(self, process: str, arguments={}) -> 'ImageCollection':
        process_id = 'apply'
        args = {
            'data': {'from_node': self.node_id},
            'process':{
                'callback':{
                    "unary":{
                        "arguments":{
                            "data": {
                                "from_argument": "dimension_data"
                            }
                        },
                        "process_id":process,
                        "result":True
                    }
                }
            }
        }

        return self.graph_add_process(process_id, args)

    def _reduce_time(self, reduce_function = "max"):
        process_id = 'reduce'

        args = {
            'data': {'from_node': self.node_id},
            'dimension': 'temporal',
            'reducer': {
                'callback': {
                    'r1': {
                        'arguments': {
                            'data': {
                                'from_argument': 'dimension_data'
                            },
                            'dimension': {
                                'from_argument': 'dimension'
                            }
                        },
                        'process_id': reduce_function,
                        'result': 'true'
                    }
                }
            }
        }

        return self.graph_add_process(process_id, args)

    def min_time(self) -> 'ImageCollection':
        """Finds the minimum value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """

        return self._reduce_time(reduce_function="min")

    def max_time(self) -> 'ImageCollection':
        """Finds the maximum value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """
        return self._reduce_time(reduce_function="max")


    def mean_time(self) -> 'ImageCollection':
        """Finds the mean value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """
        return self._reduce_time(reduce_function="mean")

    def median_time(self) -> 'ImageCollection':
        """Finds the median value of a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """

        return self._reduce_time(reduce_function="median")

    def count_time(self) -> 'ImageCollection':
        """Counts the number of images with a valid mask in a time series for all bands of the input dataset.
            :return An ImageCollection instance
        """
        return self._reduce_time(reduce_function="count")

    def ndvi(self,red=None,nir=None) -> 'ImageCollection':
        """ NDVI

            :return An ImageCollection instance
        """
        process_id = 'NDVI'

        args = {
                'data': {'from_node': self.node_id}
            }

        return self.graph_add_process(process_id, args)

    def stretch_colors(self, min, max) -> 'ImageCollection':
        """ Color stretching
        deprecated, use 'linear_scale_range' instead
            :param min: Minimum value
            :param max: Maximum value
            :return An ImageCollection instance
        """
        process_id = 'stretch_colors'
        args = {
                'data': {'from_node': self.node_id},
                'min': min,
                'max': max
            }

        return self.graph_add_process(process_id, args)



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
            'data': {'from_node': self.node_id},
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

        process_id = 'aggregate_zonal'

        args = {
                'data': {'from_node': self.node_id},
                'dimension':'temporal',
                'polygons': geojson
            }

        return self.graph_add_process(process_id, args)

    def download(self, outputfile:str, bbox="", time="", **format_options) -> str:
        """Extraxts a geotiff from this image collection."""
        self.graph[self.node_id]["result"] = 'true'
        return self.session.download(self.graph, time, outputfile, format_options)

    def tiled_viewing_service(self,**kwargs) -> Dict:
        return self.session.create_service(self.graph,**kwargs)

    def send_job(self, out_format=None, **format_options) -> Job:
        """
        Sends a job to the backend and returns a ClientJob instance.
        :param out_format: String Format of the job result.
        :param format_options: String Parameters for the job result format
        :return: status: ClientJob resulting job.
        """
        if out_format:
            return RESTJob(self.session.job({"process_graph": self.graph,
                                               'output': {
                                                   'format': out_format,
                                                   'parameters': format_options
                                               }}), self.session)
        else:
            return RESTJob(self.session.job({"process_graph": self.graph}), self.session)

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
        id = self.builder.process(process_id,args)

        return ImageCollectionClient(id,self.builder,self.session)

import copy
from distutils.version import LooseVersion
from typing import List, Dict, Union

from datetime import datetime, date

from openeo.job import Job
from openeo.rest.job import RESTJob
from ..imagecollection import ImageCollection
from ..connection import Connection
from shapely.geometry import Polygon, MultiPolygon, mapping
from ..graphbuilder import GraphBuilder


class ImageCollectionClient(ImageCollection):
    """Class representing an Image Collection. (In the API as 'imagery')
        Supports 0.4.
    """

    def __init__(self,node_id:str, builder:GraphBuilder,session:Connection):
        self.node_id = node_id
        self.builder= builder
        self.session = session
        self.graph = builder.processes
        self.bands = []

    def _isVersion040(self):
        return LooseVersion(self.session.capabilities().version()) >= LooseVersion("0.4.0")

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
            :return: An ImageCollection instance
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
            :return: An ImageCollection instance
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

    def band(self, band_name) -> 'ImageCollection':
        """Filter the imagery by the given bands
            :param bands: List of band names or single band name as a string.
            :return An ImageCollection instance
        """

        process_id = 'reduce'
        band_index = self._band_index(band_name)


        args = {
            'data': {'from_node': self.node_id},
            'dimension': 'spectral_bands',
            'reducer': {
                'callback': {
                    'r1': {
                        'arguments': {
                            'data': {
                                'from_argument': 'data'
                            },
                            'index': band_index
                        },
                        'process_id': 'array_element',
                        'result': True
                    }
                }
            }
        }

        return self.graph_add_process(process_id, args)

    def _band_index(self,name:str):
        try:
            return self.bands.index(name)
        except ValueError as e:
            raise ValueError("Given band name: " + name + " not available in this image collection. Valid band names are: " + str(self.bands))

    def subtract(self, other:Union[ImageCollection,Union[int,float]]):
        """
        Subtract other from this datacube, so the result is: this - other
        The number of bands in both data cubes has to be the same.

        :param other:
        :return ImageCollection: this - other
        """
        operator = "subtract"
        if isinstance(other, int) or isinstance(other, float):
            return self._reduce_bands_binary_const(operator, other)
        elif isinstance(other, ImageCollection):
            return self._reduce_bands_binary(operator, other)
        else:
            raise ValueError("Unsupported right-hand operand: " + str(other))

    def divide(self, other:Union[ImageCollection,Union[int,float]]):
        """
        Subtraction other from this datacube, so the result is: this - other
        The number of bands in both data cubes has to be the same.

        :param other:
        :return ImageCollection: this - other
        """
        operator = "divide"
        if isinstance(other, int) or isinstance(other, float):
            return self._reduce_bands_binary_const(operator, other)
        elif isinstance(other, ImageCollection):
            return self._reduce_bands_binary(operator, other)
        else:
            raise ValueError("Unsupported right-hand operand: " + str(other))

    def product(self, other:Union[ImageCollection,Union[int,float]]):
        """
        Multiply other with this datacube, so the result is: this * other
        The number of bands in both data cubes has to be the same.

        :param other:
        :return ImageCollection: this - other
        """
        operator = "product"
        if isinstance(other, int) or isinstance(other, float):
            return self._reduce_bands_binary_const(operator, other)
        elif isinstance(other, ImageCollection):
            return self._reduce_bands_binary(operator, other)
        else:
            raise ValueError("Unsupported right-hand operand: " + str(other))


    def __invert__(self):
        """

        :return:
        """
        operator = 'not'
        my_builder = self._get_band_graph_builder()
        new_builder = None
        extend_previous_callback_graph = my_builder != None
        if not extend_previous_callback_graph:
            new_builder = GraphBuilder()
            # TODO merge both process graphs?
            new_builder.add_process(operator, expression={'from_argument': 'data'}, result=True)
        else:
            current_result = my_builder.find_result_node_id()
            new_builder = my_builder.copy()
            new_builder.processes[current_result]['result'] = False
            new_builder.add_process(operator, expression={'from_node': current_result},  result=True)

        return self._create_reduced_collection(new_builder, extend_previous_callback_graph)

    def __ne__(self, other: Union[ImageCollection, Union[int, float]]):
        return self.__eq__(other).__invert__()



    def __eq__(self, other:Union[ImageCollection,Union[int,float]]):
        """
        Pixelwise comparison of this data cube with another cube or constant.

        :param other: Another data cube, or a constant
        :return:
        """
        operator = "eq"
        if isinstance(other, int) or isinstance(other, float):
            my_builder = self._get_band_graph_builder()
            new_builder = None
            extend_previous_callback_graph = my_builder != None
            if not extend_previous_callback_graph:
                new_builder = GraphBuilder()
                # TODO merge both process graphs?
                new_builder.add_process(operator, x={'from_argument': 'data'}, y = other, result=True)
            else:
                current_result = my_builder.find_result_node_id()
                new_builder = my_builder.copy()
                new_builder.processes[current_result]['result'] = False
                new_builder.add_process(operator, x={'from_node': current_result}, y = other, result=True)

            return self._create_reduced_collection(new_builder, extend_previous_callback_graph)
        elif isinstance(other, ImageCollection):
            return self._reduce_bands_binary(operator, other)
        else:
            raise ValueError("Unsupported right-hand operand: " + str(other))

    def _create_reduced_collection(self, callback_graph_builder, extend_previous_callback_graph):
        if not extend_previous_callback_graph:
            # there was no previous reduce step
            args = {
                'data': {'from_node': self.node_id},
                'dimension': 'spectral_bands',
                'reducer': {
                    'callback': callback_graph_builder.processes
                }
            }
            return self.graph_add_process("reduce", args)
        else:
            process_graph_copy = self.builder.copy()
            process_graph_copy.processes[self.node_id]['arguments']['reducer']['callback'] = callback_graph_builder.processes

            # now current_node should be a reduce node, let's modify it
            return ImageCollectionClient(self.node_id, process_graph_copy, self.session)

    def __truediv__(self,other):
        return self.divide(other)

    def __sub__(self, other):
        return self.subtract(other)

    def __radd__(self, other):
        return self.add(other)

    def __add__(self, other):
        return self.add(other)

    def __mul__(self, other):
        return self.product(other)

    def __rmul__(self, other):
        return self.product(other)

    def add(self, other:Union[ImageCollection,Union[int,float]]):
        """
        Pairwise addition of the bands in this data cube with the bands in the 'other' data cube.
        The number of bands in both data cubes has to be the same.

        :param other:
        :return ImageCollection: this + other
        """
        operator = "sum"
        if isinstance(other, int) or isinstance(other, float):
            return self._reduce_bands_binary_const(operator, other)
        elif isinstance(other, ImageCollection):
            return self._reduce_bands_binary(operator, other)
        else:
            raise ValueError("Unsupported right-hand operand: " + str(other))

    def _reduce_bands_binary(self, operator, other):
        # first we create the callback
        my_builder = self._get_band_graph_builder()
        other_builder = other._get_band_graph_builder()
        input_node = {'from_argument': 'data'}
        if my_builder == None or other_builder == None:
            if my_builder == None and other_builder == None:
                merged = GraphBuilder()
                # TODO merge both process graphs?
                merged.add_process( operator, data=[input_node, input_node], result=True)
            else:
                merged = my_builder or other_builder
                merged = merged.copy()
                current_result = merged.find_result_node_id()
                merged.processes[current_result]['result'] = False
                merged.add_process(operator, data=[input_node, {'from_node': current_result}], result=True)
        else:
            input1 = my_builder.processes[my_builder.find_result_node_id()]
            merged = my_builder.merge(other_builder)
            input1_id = list(merged.processes.keys())[list(merged.processes.values()).index(input1)]
            merged.processes[input1_id]['result'] = False
            input2_id = merged.find_result_node_id()
            input2 = merged.processes[input2_id]
            input2['result'] = False
            merged.add_process(operator, data=[{'from_node': input1_id}, {'from_node': input2_id}], result=True)
        # callback is ready, now we need to properly set up the reduce process that will invoke it
        if my_builder == None and other_builder == None:
            # there was no previous reduce step
            args = {
                'data': {'from_node': self.node_id},
                'process': {
                    'callback': merged.processes
                }
            }
            return self.graph_add_process("reduce", args)
        else:
            node_id = self.node_id
            reducing_graph = self
            if reducing_graph.graph[node_id]["process_id"] != "reduce":
                node_id = other.node_id
                reducing_graph = other
            new_builder = reducing_graph.builder.copy()
            new_builder.processes[node_id]['arguments']['reducer']['callback'] = merged.processes
            # now current_node should be a reduce node, let's modify it
            return ImageCollectionClient(node_id, new_builder, reducing_graph.session)

    def _reduce_bands_binary_const(self, operator, other:Union[int,float]):
        my_builder = self._get_band_graph_builder()
        new_builder = None
        extend_previous_callback_graph = my_builder !=None
        if not extend_previous_callback_graph:
            new_builder = GraphBuilder()
            # TODO merge both process graphs?
            new_builder.add_process(operator, data=[{'from_argument': 'data'}, other], result=True)
        else:
            current_result = my_builder.find_result_node_id()
            new_builder = my_builder.copy()
            new_builder.processes[current_result]['result'] = False
            new_builder.add_process(operator, data=[{'from_node': current_result}, other], result=True)

        return self._create_reduced_collection(new_builder,extend_previous_callback_graph)


    def _get_band_graph_builder(self):
        current_node = self.graph[self.node_id]
        if current_node["process_id"] == "reduce":
            if current_node["arguments"]["dimension"] == "spectral_bands":
                callback_graph = current_node["arguments"]["reducer"]["callback"]
                return GraphBuilder(graph=callback_graph)
        return None


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

    def apply_dimension(self, code: str, runtime=None, version="latest",dimension='temporal') -> 'ImageCollection':
        """
        Applies an n-ary process (i.e. takes an array of pixel values instead of a single pixel value) to a raster data cube.
        In contrast, the process apply applies an unary process to all pixel values.

        By default, apply_dimension applies the the process on all pixel values in the data cube as apply does, but the parameter dimension can be specified to work only on a particular dimension only. For example, if the temporal dimension is specified the process will work on a time series of pixel values.

        The n-ary process must return as many elements in the returned array as there are in the input array. Otherwise a CardinalityChanged error must be returned.


        :param code: UDF code or process identifier
        :param runtime:
        :param version:
        :param dimension:
        :return:
        :raises: CardinalityChangedError
        """

        if self._isVersion040():
            process_id = 'apply_dimension'
            if runtime !=None:

                callback = {
                    'udf': self._create_run_udf(code, runtime, version)
                }
            else:
                callback = {
                    'process': {
                        "arguments": {
                            "data": {
                                "from_argument": "data"
                            }
                        },
                        "process_id": code,
                        "result": True
                    }
                }
            args = {
                'data': {
                    'from_node': self.node_id
                },
                'dimension': dimension,

                'process': {
                    'callback': callback
                }
            }
            return self.graph_add_process(process_id, args)
        else:
            raise NotImplementedError("apply_dimension requires backend version >=0.4.0")


    def apply_tiles(self, code: str,runtime="Python",version="latest") -> 'ImageCollection':
        """Apply a function to the given set of tiles in this image collection.

            This type applies a simple function to one pixel of the input image or image collection.
            The function gets the value of one pixel (including all bands) as input and produces a single scalar or tuple output.
            The result has the same schema as the input image (collection) but different bands.
            Examples include the computation of vegetation indexes or filtering cloudy pixels.

            Code should follow the OpenEO UDF conventions.

            :param code: String representing Python code to be executed in the backend.
        """

        if self._isVersion040():
            process_id = 'reduce'
            args = {
                'data': {
                    'from_node': self.node_id
                },
                'dimension': 'spectral_bands',#TODO determine dimension based on datacube metadata
                'binary': 'false',
                'reducer': {
                    'callback': {
                        'udf': self._create_run_udf(code, runtime, version)
                    }
                }
            }
        else:

            process_id = 'apply_tiles'
            args = {
                    'data': {'from_node': self.node_id},
                    'code':{
                        'language':'python',
                        'source':code
                    }
                }

        return self.graph_add_process(process_id, args)

    def _create_run_udf(self, code, runtime, version):
        return {
            "arguments": {
                "data": {
                    "from_argument": "data"
                },
                "runtime": runtime,
                "version": version,
                "udf": code

            },
            "process_id": "run_udf",
            "result": True
        }

    #TODO better name, pull to ABC?
    def reduce_tiles_over_time(self,code: str,runtime="Python",version="latest"):
        """
        Applies a user defined function to a timeseries of tiles. The size of the tile is backend specific, and can be limited to one pixel.
        The function should reduce the given timeseries into a single (multiband) tile.

        :param code: The UDF code, compatible with the given runtime and version
        :param runtime: The UDF runtime
        :param version: The UDF runtime version
        :return:
        """
        if self._isVersion040():
            process_id = 'reduce'
            args = {
                'data': {
                    'from_node': self.node_id
                },
                'dimension': 'temporal',#TODO determine dimension based on datacube metadata
                'binary': False,
                'reducer': {
                    'callback': {
                        'udf': self._create_run_udf(code, runtime, version)
                    }
                }
            }
            return self.graph_add_process(process_id, args)
        else:
            raise NotImplementedError("apply_to_tiles_over_time requires backend version >=0.4.0")


    def apply(self, process: str, data_argument='data',arguments={}) -> 'ImageCollection':
        process_id = 'apply'
        arguments[data_argument] = \
            {
                "from_argument": "data"
            }
        args = {
            'data': {'from_node': self.node_id},
            'process':{
                'callback':{
                    "unary":{
                        "arguments":arguments,
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
                                'from_argument': 'data'
                            }
                        },
                        'process_id': reduce_function,
                        'result': True
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



    def mask(self, polygon: Union[Polygon, MultiPolygon]=None, srs="EPSG:4326",rastermask:'ImageCollection'=None,replacement=None) -> 'ImageCollection':
        """
        Mask the image collection using a polygon. All pixels outside the polygon should be set to the nodata value.
        All pixels inside, or intersecting the polygon should retain their original value.

        :param polygon: A polygon, provided as a Shapely Polygon or MultiPolygon
        :param srs: The reference system of the provided polygon, by default this is Lat Lon (EPSG:4326).
        :return: A new ImageCollection, with the mask applied.
        """
        mask = None
        new_collection = None
        if polygon is not None:
            geojson = mapping(polygon)
            geojson['crs'] = {
                'type': 'name',
                'properties': {
                    'name': srs
                }
            }
            mask = geojson
            new_collection = self
        elif rastermask is not None:
            mask_node = rastermask.graph[rastermask.node_id]
            mask_node['result']=True
            new_collection = self._graph_merge(rastermask.graph)
            #mask node id may have changed!
            mask_id = new_collection.builder.find_result_node_id()
            mask_node = new_collection.graph[mask_id]
            mask_node['result']=False
            mask = {
                'from_node': mask_id
            }

        else:
            raise AttributeError("mask process: either a polygon or a rastermask should be provided.")

        process_id = 'mask'

        args = {
            'data': {'from_node': self.node_id},
            'mask': mask
        }
        if replacement is not None:
            args['replacement'] = replacement

        return new_collection.graph_add_process(process_id, args)

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
        if self._isVersion040():
            process_id = 'aggregate_polygon'

        args = {
                'data': {'from_node': self.node_id},
                'dimension':'temporal',
                'polygons': geojson
            }
        if self._isVersion040():
            del args['dimension']
            args['reducer']= {
                'callback':{
                    "unary":{
                        "arguments":{
                            "data": {
                                "from_argument": "data"
                            }
                        },
                        "process_id":"mean",
                        "result":True
                    }
                }
            }

        return self.graph_add_process(process_id, args)

    def download(self, outputfile:str, bbox="", time="", **format_options) -> str:
        """Extraxts a geotiff from this image collection."""

        if self._isVersion040():
            args = {
                'data': {'from_node': self.node_id},
                'options': format_options
            }
            if 'format' in format_options:
                args['format'] = format_options['format']
                del format_options['format']
            newcollection = self.graph_add_process("save_result",args)
            newcollection.graph[newcollection.node_id]["result"] = 'true'
            return self.session.download(newcollection.graph, time, outputfile, format_options)
        else:
            self.graph[self.node_id]["result"] = 'true'
            return self.session.download(self.graph, time, outputfile, format_options)

    def tiled_viewing_service(self,**kwargs) -> Dict:
        self.graph[self.node_id]["result"] = 'true'
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
        self.graph[self.node_id]['result'] = True
        return self.session.execute({"process_graph": self.graph},"")

    ####### HELPER methods #######

    def _graph_merge(self, other_graph:Dict):
        newbuilder = GraphBuilder(self.builder.processes)
        merged = newbuilder.merge(GraphBuilder(other_graph))
        newCollection = ImageCollectionClient(self.node_id, merged, self.session)
        newCollection.bands = self.bands
        return newCollection


    def graph_add_process(self, process_id, args) -> 'ImageCollection':
        """
        Returns a new restimagery with an added process with the given process
        id and a dictionary of arguments
        :param process_id: String, Process Id of the added process.
        :param args: Dict, Arguments of the process.
        :return: imagery: Instance of the RestImagery class
        """
        #don't modify in place, return new builder
        newbuilder = GraphBuilder(self.builder.processes)
        id = newbuilder.process(process_id,args)

        newCollection = ImageCollectionClient(id, newbuilder, self.session)
        newCollection.bands = self.bands
        return newCollection

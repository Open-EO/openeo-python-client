import typing
from typing import List, Dict, Union, Sequence

from deprecated import deprecated
from shapely.geometry import Polygon, MultiPolygon, mapping

from openeo.graphbuilder import GraphBuilder
from openeo.imagecollection import ImageCollection, CollectionMetadata
from openeo.job import Job

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection


class ImageCollectionClient(ImageCollection):
    """Class representing an Image Collection. (In the API as 'imagery')
        Supports 0.4.
    """

    def __init__(self, node_id: str, builder: GraphBuilder, session: 'Connection', metadata: CollectionMetadata=None):
        super().__init__(metadata=metadata)
        self.node_id = node_id
        self.builder= builder
        self.session = session
        self.graph = builder.processes
        self.metadata = metadata

    @property
    def _api_version(self):
        return self.session.capabilities().api_version_check

    @classmethod
    def load_collection(
            cls, collection_id: str, session: 'Connection' = None,
            spatial_extent: Union[Dict[str, float], None] = None,
            temporal_extent: Union[List[str], None] = None,
            bands: Union[List[str], None] = None,
            fetch_metadata=True
    ):
        """
        Create a new Image Collection/Raster Data cube.

        :param collection_id: A collection id, should exist in the backend.
        :param session: The session to use to connect with the backend.
        :param spatial_extent: limit data to specified bounding box or polygons
        :param temporal_extent: limit data to specified temporal interval
        :param bands: only add the specified bands
        :return:
        """
        # TODO: rename function to load_collection for better similarity with corresponding process id?
        assert session.capabilities().api_version_check.at_least('0.4.0')
        builder = GraphBuilder()
        process_id = 'load_collection'
        arguments = {
            'id': collection_id,
            'spatial_extent': spatial_extent,
            'temporal_extent': temporal_extent,
        }
        if bands:
            arguments['bands'] = bands
        node_id = builder.process(process_id, arguments)
        metadata = session.collection_metadata(collection_id) if fetch_metadata else None
        return cls(node_id, builder, session, metadata=metadata)

    @classmethod
    @deprecated("use load_collection instead")
    def create_collection(cls, *args, **kwargs):
        return cls.load_collection(*args, **kwargs)

    @classmethod
    def load_disk_collection(cls, session: 'Connection', file_format: str, glob_pattern: str, **options) -> 'ImageCollection':
        """
        Loads image data from disk as an ImageCollection.

        :param session: The session to use to connect with the backend.
        :param file_format: the file format, e.g. 'GTiff'
        :param glob_pattern: a glob pattern that matches the files to load from disk
        :param options: options specific to the file format
        :return: the data as an ImageCollection
        """
        assert session.capabilities().api_version_check.at_least('0.4.0')

        builder = GraphBuilder()

        process_id = 'load_disk_data'
        arguments = {
            'format': file_format,
            'glob_pattern': glob_pattern,
            'options': options
        }

        node_id = builder.process(process_id, arguments)

        return cls(node_id, builder, session, metadata={})

    def _filter_temporal(self, start: str, end: str) -> 'ImageCollection':
        return self.graph_add_process(
            process_id='filter_temporal',
            args={
                'data': {'from_node': self.node_id},
                'extent': [start, end]
            }
        )

    def filter_bbox(self, west, east, north, south, crs=None, base=None, height=None) -> 'ImageCollection':
        extent = {
            'west': west, 'east': east, 'north': north, 'south': south,
            'crs': crs,
        }
        if base is not None or height is not None:
            extent.update(base=base, height=height)
        return self.graph_add_process(
            process_id='filter_bbox',
            args={
                'data': {'from_node': self.node_id},
                'extent': extent
            }
        )

    def filter_bands(self, bands: List[Union[str, int]]) -> ImageCollection:
        """Filter the imagery by the given bands
            :param bands: List of band names or single band name as a string.
            :return An ImageCollection instance
        """
        return self.graph_add_process(
            process_id='filter_bands',
            args={
                'data': {'from_node': self.node_id},
                'bands': bands
            }
        )

    @deprecated("use `filter_bands()` instead")
    def band_filter(self, bands) -> ImageCollection:
        return self.filter_bands(bands)

    def band(self, band: Union[str, int]) -> 'ImageCollection':
        """Filter the imagery by the given bands
            :param band: band name, band common name or band index.
            :return An ImageCollection instance
        """

        process_id = 'reduce'
        band_index = self.metadata.get_band_index(band)

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

    def resample_spatial(self, resolution: Union[Union[int, float], Sequence[Union[int, float]]],
                         projection: Union[int, str] = None, method: str = 'near', align: str = 'lower-left'):
        return self.graph_add_process('resample_spatial', {
            'data': {'from_node': self.node_id},
            'resolution': resolution,
            'projection': projection,
            'method': method,
            'align': align
        })

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

    def logical_or(self, other: ImageCollection):
        """
        Apply element-wise logical `or` operation
        :param other:
        :return ImageCollection: logical_or(this, other)
        """
        return self._reduce_bands_binary(operator='or', other=other,arg_name='expressions')

    def logical_and(self, other: ImageCollection):
        """
        Apply element-wise logical `and` operation
        :param other:
        :return ImageCollection: logical_and(this, other)
        """
        return self._reduce_bands_binary(operator='and', other=other,arg_name='expressions')

    def __invert__(self):
        """

        :return:
        """
        operator = 'not'
        my_builder = self._get_band_graph_builder()
        new_builder = None
        extend_previous_callback_graph = my_builder is not None
        # TODO: why does these `add_process` calls use "expression" instead of "data" like the other cases?
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
        return self._reduce_bands_binary_xy('eq',other)
    
    def __gt__(self, other:Union[ImageCollection,Union[int,float]]):
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.
        The number of bands in both data cubes has to be the same.

        :param other:
        :return ImageCollection: this + other
        """
        return self._reduce_bands_binary_xy('gt',other)
            
    def __lt__(self, other:Union[ImageCollection,Union[int,float]]):
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.
        The number of bands in both data cubes has to be the same.

        :param other:
        :return ImageCollection: this + other
        """
        return self._reduce_bands_binary_xy('lt',other)

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
            # TODO: set metadata of reduced cube?
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

    def __or__(self, other):
        return self.logical_or(other)

    def __and__(self, other):
        return  self.logical_and(other)

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

    def _reduce_bands_binary(self, operator, other: 'ImageCollectionClient',arg_name='data'):
        # first we create the callback
        fallback_node = {'from_argument': 'data'}
        my_builder = self._get_band_graph_builder()
        other_builder = other._get_band_graph_builder()
        merged = GraphBuilder.combine(operator=operator,
                                      first=my_builder or fallback_node,
                                      second=other_builder or fallback_node, arg_name=arg_name)
        # callback is ready, now we need to properly set up the reduce process that will invoke it
        if my_builder is None and other_builder is None:
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
            # TODO: set metadata of reduced cube?
            return ImageCollectionClient(node_id, new_builder, reducing_graph.session)
        
    def _reduce_bands_binary_xy(self,operator,other:Union[ImageCollection,Union[int,float]]):
        """
        Pixelwise comparison of this data cube with another cube or constant.

        :param other: Another data cube, or a constant
        :return:
        """        
        if isinstance(other, int) or isinstance(other, float):
            my_builder = self._get_band_graph_builder()
            new_builder = None
            extend_previous_callback_graph = my_builder is not None
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

    def _reduce_bands_binary_const(self, operator, other:Union[int,float]):
        my_builder = self._get_band_graph_builder()
        new_builder = None
        extend_previous_callback_graph = my_builder is not None
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

        if self._api_version.at_least('0.4.0'):
            process_id = 'apply_dimension'
            if runtime:

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

        if self._api_version.at_least('0.4.0'):
            process_id = 'reduce'
            args = {
                'data': {
                    'from_node': self.node_id
                },
                'dimension': 'spectral_bands',#TODO determine dimension based on datacube metadata
                'binary': False,
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
        if self._api_version.at_least('0.4.0'):
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
                "from_argument": data_argument
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

    def ndvi(self, name="ndvi") -> 'ImageCollection':
        """ NDVI

            :return An ImageCollection instance
        """
        process_id = 'ndvi'
        args = {
            'data': {'from_node': self.node_id},
            'name': name
        }
        return self.graph_add_process(process_id, args)

    @deprecated("use 'linear_scale_range' instead")
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
    
    def linear_scale_range(self, inputMin, inputMax, outputMin, outputMax) -> 'ImageCollection':
        """ Color stretching
            :param inputMin: Minimum input value
            :param inputMax: Maximum input value
            :param outputMin: Minimum output value
            :param outputMax: Maximum output value
            :return An ImageCollection instance
        """
        process_id = 'linear_scale_range'
        args = {
                'x': {'from_node': self.node_id},
                'inputMin': inputMin,
                'inputMax': inputMax,
                'outputMin': outputMin,
                'outputMax': outputMax
            }

        return self.graph_add_process(process_id, args)

    def mask(self, polygon: Union[Polygon, MultiPolygon]=None, srs="EPSG:4326", rastermask: 'ImageCollection'=None,
             replacement=None) -> 'ImageCollection':
        """
        Mask the image collection using either a polygon or a raster mask.

        All pixels outside the polygon should be set to the nodata value.
        All pixels inside, or intersecting the polygon should retain their original value.

        All pixels are replaced for which the corresponding pixels in the mask are non-zero (for numbers) or True
        (for boolean values).

        The pixel values are replaced with the value specified for replacement, which defaults to None (no data).
        No data values will be left untouched by the masking operation.

        # TODO: just provide a single `mask` argument and detect the type: polygon or process graph

        :param polygon: A polygon, provided as a :class:`shapely.geometry.Polygon` or :class:`shapely.geometry.MultiPolygon`
        :param srs: The reference system of the provided polygon, by default this is Lat Lon (EPSG:4326).
        :param rastermask: the raster mask
        :param replacement: the value to replace the masked pixels with
        :raise: :class:`ValueError` if a polygon is supplied and its area is 0.
        :return: A new ImageCollection, with the mask applied.
        """
        mask = None
        new_collection = None
        if polygon is not None:
            if polygon.area == 0:
                raise ValueError("Mask {m!s} has an area of {a!r}".format(m=polygon, a=polygon.area))

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

    def apply_kernel(self, kernel, factor=1.0) -> 'ImageCollection':
        """
        Applies a focal operation based on a weighted kernel to each value of the specified dimensions in the data cube.

        :param kernel: The kernel to be applied on the data cube. It should be a 2D numpy array.
        :param factor: A factor that is multiplied to each value computed by the focal operation. This is basically a shortcut for explicitly multiplying each value by a factor afterwards, which is often required for some kernel-based algorithms such as the Gaussian blur.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        return self.graph_add_process('apply_kernel', {
            'data': {'from_node': self.node_id},
            'kernel':kernel.tolist(),
            'factor':factor
        })

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
        self.graph[self.node_id]['result'] = True
        return self.session.point_timeseries({"process_graph": self.graph}, x, y, srs)

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'ImageCollection':
        """
        Extract a mean time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: ImageCollection
        """

        return self._polygonal_timeseries(polygon, "mean")

    def polygonal_histogram_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> Dict:
        """
        Extract a histogram time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: ImageCollection
        """

        return self._polygonal_timeseries(polygon, "histogram")

    def polygonal_median_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> Dict:
        """
        Extract a median time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: ImageCollection
        """

        return self._polygonal_timeseries(polygon, "median")

    def polygonal_standarddeviation_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> Dict:
        """
        Extract a time series of standard deviations for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: ImageCollection
        """

        return self._polygonal_timeseries(polygon, "sd")

    def _polygonal_timeseries(self, polygon: Union[Polygon, MultiPolygon, str], func: str) -> 'ImageCollection':
        def graph_add_aggregate_process(graph) -> 'ImageCollection':
            process_id = 'aggregate_zonal'
            if self._api_version.at_least('0.4.0'):
                process_id = 'aggregate_polygon'

            args = {
                'data': {'from_node': self.node_id},
                'dimension': 'temporal',
                'polygons': polygons
            }
            if self._api_version.at_least('0.4.0'):
                del args['dimension']
                args['reducer'] = {
                    'callback': {
                        "unary": {
                            "arguments": {
                                "data": {
                                    "from_argument": "data"
                                }
                            },
                            "process_id": func,
                            "result": True
                        }
                    }
                }

            return graph.graph_add_process(process_id, args)

        if isinstance(polygon, str):
            if self._api_version.at_least('0.4.0'):
                with_read_vector = self.graph_add_process('read_vector', args={
                    'filename': polygon
                })

                polygons = {
                    'from_node': with_read_vector.node_id
                }

                return graph_add_aggregate_process(with_read_vector)
            else:
                raise NotImplementedError("filename requires backend version >=0.4.0")
        else:
            polygons = mapping(polygon)
            polygons['crs'] = {
                'type': 'name',
                'properties': {
                    'name': 'EPSG:4326'
                }
            }

            return graph_add_aggregate_process(self)

    def save_result(self, format: str, options: dict = None):
        return self.graph_add_process(
            process_id="save_result",
            args={
                "data": {"from_node": self.node_id},
                "format": format,
                "options": options
            }
        )

    def download(self, outputfile: str, **format_options) -> str:
        """Extracts a geotiff from this image collection."""

        if self._api_version.at_least('0.4.0'):
            args = {
                'data': {'from_node': self.node_id},
                'options': format_options
            }
            if 'format' in format_options:
                args['format'] = format_options.pop('format')
            else:
                raise ValueError("Please use the 'format' keyword argument to specify the output format. Use openeo.connection.Connection#list_file_types to retrieve available ouput formats for this backend.")
            newcollection = self.graph_add_process("save_result",args)
            newcollection.graph[newcollection.node_id]["result"] = True
            return self.session.download(newcollection.graph, outputfile, format_options)
        else:
            self.graph[self.node_id]["result"] = True
            return self.session.download(self.graph, outputfile, format_options)

    def tiled_viewing_service(self,**kwargs) -> Dict:
        newbuilder = self.builder.copy()
        newbuilder.processes[self.node_id]['result'] = True
        return self.session.create_service(newbuilder.processes,**kwargs)

    def send_job(self, out_format=None, **format_options) -> Job:
        """
        Sends a job to the backend and returns a ClientJob instance.
        :param out_format: String Format of the job result.
        :param format_options: String Parameters for the job result format
        :return: status: ClientJob resulting job.
        """
        if out_format:
            graph = self.graph
            if self._api_version.at_least('0.4.0'):
                args = {
                    'data': {'from_node': self.node_id},
                    'options': format_options,
                    'format': out_format
                }
                newcollection = self.graph_add_process("save_result", args)
                newcollection.graph[newcollection.node_id]["result"] = True
                return self.session.create_job(process_graph=newcollection.graph)
            else:
                return self.session.create_job(process_graph=graph, output_format=out_format,
                                               output_parameters=format_options)

        else:
            return self.session.create_job(process_graph=self.graph)

    def execute(self) -> Dict:
        """Executes the process graph of the imagery. """
        newbuilder = self.builder.copy()
        newbuilder.processes[self.node_id]['result'] = True
        return self.session.execute({"process_graph": newbuilder.processes},"")

    ####### HELPER methods #######

    def _graph_merge(self, other_graph:Dict):
        newbuilder = GraphBuilder(self.builder.processes)
        merged = newbuilder.merge(GraphBuilder(other_graph))
        # TODO: properly update metadata as well?
        newCollection = ImageCollectionClient(self.node_id, merged, self.session, metadata=self.metadata)
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

        # TODO: properly update metadata as well?
        newCollection = ImageCollectionClient(id, newbuilder, self.session, metadata=self.metadata)
        return newCollection

    def to_graphviz(self):
        """
        Build a graphviz DiGraph from the process graph
        :return:
        """
        import graphviz
        import pprint

        graph = graphviz.Digraph(node_attr={"shape": "none", "fontname": "sans", "fontsize": "11"})
        for name, process in self.graph.items():
            args = process.get("arguments", {})
            # Build label
            label = '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">'
            label += '<TR><TD COLSPAN="2" BGCOLOR="#eeeeee">{pid}</TD></TR>'.format(pid=process["process_id"])
            label += "".join(
                '''<TR><TD ALIGN="RIGHT">{arg}</TD>
                       <TD ALIGN="LEFT"><FONT FACE="monospace">{value}</FONT></TD></TR>'''.format(
                    arg=k, value=pprint.pformat(v)[:1000].replace('\n', '<BR/>')
                ) for k, v in sorted(args.items())
            )
            label += '</TABLE>>'
            # Add node and edges to graph
            graph.node(name, label=label)
            if "data" in args and "from_node" in args["data"]:
                graph.edge(args["data"]["from_node"], name)

            # TODO: add subgraph for "callback" arguments?

        return graph

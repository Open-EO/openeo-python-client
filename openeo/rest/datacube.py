"""
The main module for creating earth observation processes. It aims to easily build complex process chains, that can
be evaluated by an openEO backend.

.. data:: THIS

    Symbolic reference to the current data cube, to be used as argument in DataCube.process() calls

"""
import datetime
import logging
import pathlib
import typing
from typing import List, Dict, Union, Tuple

import numpy as np
import shapely.geometry
import shapely.geometry.base
from deprecated import deprecated
from shapely.geometry import Polygon, MultiPolygon, mapping

from openeo.imagecollection import ImageCollection, CollectionMetadata
from openeo.internal.graph_building import PGNode, ReduceNode
from openeo.rest import BandMathException, OperatorException
from openeo.rest.job import RESTJob
from openeo.rest.udp import RESTUserDefinedProcess
from openeo.util import get_temporal_extent, dict_no_none
from openeo.vectorcube import VectorCube
from openeo.metadata import Band

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection

log = logging.getLogger(__name__)

# Sentinel object to refer to "current" cube in chained process expressions.
THIS = object()


class DataCube(ImageCollection):
    """
    Class representing a openEO Data Cube. Data loaded from the backend is returned as an object of this class.
    Various processing methods can be invoked to build a complete workflow.

    Supports openEO API 1.0.
    In earlier versions this was called `ImageCollectionClient`
    """

    def __init__(self, graph: PGNode, connection: 'Connection', metadata: CollectionMetadata = None):
        super().__init__(metadata=metadata)
        # Process graph
        self._pg = graph
        self._connection = connection
        self.metadata = metadata

    def __str__(self):
        return "DataCube({pg})".format(pg=self._pg)

    @property
    def graph(self) -> dict:
        """Get the process graph in flattened dict representation"""
        return self.flatten()

    def flatten(self) -> dict:
        """Get the process graph in flattened dict representation"""
        return self._pg.flatten()

    @property
    def _api_version(self):
        return self._connection.capabilities().api_version_check

    @property
    def connection(self):
        return self._connection

    def process(self, process_id: str, arguments: dict = None, metadata: CollectionMetadata = None, **kwargs) -> 'DataCube':
        """
        Generic helper to create a new DataCube by applying a process.

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :return: new DataCube instance
        """
        arguments = {**(arguments or {}), **kwargs}
        for k, v in arguments.items():
            if isinstance(v, DataCube):
                arguments[k] = {"from_node": v._pg}
            elif v is THIS:
                arguments[k] = {"from_node": self._pg}
        return self.process_with_node(PGNode(
            process_id=process_id,
            arguments=arguments,
        ), metadata=metadata)

    # Legacy `graph_add_node` method
    graph_add_node = deprecated(reason="just use `process()`")(process)

    def process_with_node(self, pg: PGNode, metadata: CollectionMetadata = None) -> 'DataCube':
        """
        Generic helper to create a new DataCube by applying a process (given as process graph node)

        :param pg: process graph node (containing process id and arguments)
        :param metadata: (optional) metadata to override original cube metadata (e.g. when reducing dimensions)
        :return: new DataCube instance
        """
        # TODO: deep copy `self.metadata` instead of using same instance?
        # TODO: cover more cases where metadata has to be altered
        return DataCube(graph=pg, connection=self._connection, metadata=metadata or self.metadata)

    @classmethod
    def load_collection(
            cls, collection_id: str, connection: 'Connection' = None,
            spatial_extent: Union[Dict[str, float], None] = None,
            temporal_extent: Union[List[Union[str, datetime.datetime, datetime.date]], None] = None,
            bands: Union[List[str], None] = None,
            fetch_metadata=True,
            properties: Dict[str, PGNode] = None
    ):
        """
        Create a new Raster Data cube.

        :param collection_id: A collection id, should exist in the backend.
        :param connection: The connection to use to connect with the backend.
        :param spatial_extent: limit data to specified bounding box or polygons
        :param temporal_extent: limit data to specified temporal interval
        :param bands: only add the specified bands
        :param properties: limit data by metadata property predicates
        :return:
        """
        normalized_temporal_extent = list(
            get_temporal_extent(extent=temporal_extent)) if temporal_extent is not None else None
        arguments = {
            'id': collection_id,
            'spatial_extent': spatial_extent,
            'temporal_extent': normalized_temporal_extent,
        }
        metadata = connection.collection_metadata(collection_id) if fetch_metadata else None
        if bands:
            if isinstance(bands, str):
                bands = [bands]
            if metadata:
                bands = [metadata.band_dimension.band_name(b) for b in bands]
            arguments['bands'] = bands
        if properties:
            arguments['properties'] = {
                prop: PGNode.to_process_graph_argument(pred)
                for prop, pred in properties.items()
            }
        pg = PGNode(
            process_id='load_collection',
            arguments=arguments
        )
        if bands:
            metadata = metadata.filter_bands(bands)
        return cls(graph=pg, connection=connection, metadata=metadata)

    @classmethod
    @deprecated("use load_collection instead")
    def create_collection(cls, *args, **kwargs):
        return cls.load_collection(*args, **kwargs)

    @classmethod
    def load_disk_collection(cls, connection: 'Connection', file_format: str, glob_pattern: str,
                             **options) -> 'DataCube':
        """
        Loads image data from disk as a DataCube.

        :param connection: The connection to use to connect with the backend.
        :param file_format: the file format, e.g. 'GTiff'
        :param glob_pattern: a glob pattern that matches the files to load from disk
        :param options: options specific to the file format
        :return: the data as a DataCube
        """
        pg = PGNode(
            process_id='load_disk_data',
            arguments={
                'format': file_format,
                'glob_pattern': glob_pattern,
                'options': options
            }
        )
        return cls(graph=pg, connection=connection, metadata={})

    def _filter_temporal(self, start: str, end: str) -> 'DataCube':
        return self.process(
            process_id='filter_temporal',
            arguments={
                'data': THIS,
                'extent': [start, end]
            }
        )

    def filter_bbox(self, west, east, north, south, crs=None, base=None, height=None) -> 'DataCube':
        extent = {
            'west': west, 'east': east, 'north': north, 'south': south,
            'crs': crs,
        }
        if base is not None or height is not None:
            extent.update(base=base, height=height)
        return self.process(
            process_id='filter_bbox',
            arguments={
                'data': THIS,
                'extent': extent
            }
        )

    def filter_bands(self, bands: Union[List[Union[str, int]], str]) -> 'DataCube':
        """
        Filter the data cube by the given bands
        :param bands: list of band names, common names or band indices. Single band name can also be given as string.
        :return a DataCube instance
        """
        if isinstance(bands, str):
            bands = [bands]
        bands = [self.metadata.band_dimension.band_name(b) for b in bands]
        cube = self.process(
            process_id='filter_bands',
            arguments={'data': THIS, 'bands': bands}
        )
        if cube.metadata:
            cube.metadata = cube.metadata.filter_bands(bands)
        return cube

    @deprecated("use `filter_bands()` instead")
    def band_filter(self, bands) -> 'DataCube':
        return self.filter_bands(bands)

    def band(self, band: Union[str, int]) -> 'DataCube':
        """Filter the imagery by the given bands
            :param band: band name, band common name or band index.
            :return a DataCube instance
        """
        band_index = self.metadata.get_band_index(band)
        return self._reduce_bands(reducer=PGNode(
            process_id='array_element',
            arguments={
                'data': {'from_parameter': 'data'},
                'index': band_index
            },
        ))

    def resample_spatial(
            self, resolution: Union[float, Tuple[float, float]], projection: Union[int, str] = None,
            method: str = 'near', align: str = 'upper-left'
    ):
        return self.process('resample_spatial', {
            'data': THIS,
            'resolution': resolution,
            'projection': projection,
            'method': method,
            'align': align
        })

    def resample_cube_spatial(self, target: 'DataCube' , method: str = 'near'):
        return self.process('resample_cube_spatial', {
            'data': THIS,
            'target': {'from_node': target._pg},
            'method': method
        })

    def _operator_binary(self, operator: str, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        """Generic handling of (mathematical) binary operator"""
        band_math_mode = self._in_bandmath_mode()
        if band_math_mode:
            if isinstance(other, (int, float)):
                return self._bandmath_operator_binary_scalar(operator, other, reverse=reverse)
            elif isinstance(other, DataCube):
                return self._bandmath_operator_binary_cubes(operator, other)
        else:
            if isinstance(other, DataCube):
                return self._merge_operator_binary_cubes(operator, other)
            elif isinstance(other, (int, float)) and not reverse:
                # TODO #123: support appending to pre-existing apply process instead of adding a whole new one
                return self.apply(process=PGNode(
                    process_id=operator,
                    arguments={"x": {"from_parameter": "x"}, "y": other}
                ))
        raise OperatorException("Unsupported operator {op!r} with {other!r} (band math mode={b})".format(
            op=operator, other=other, b=band_math_mode))

    def _operator_unary(self, operator: str) -> 'DataCube':
        band_math_mode = self._in_bandmath_mode()
        if band_math_mode:
            return self._bandmath_operator_unary(operator)
        raise OperatorException("Unsupported unary operator {op!r} (band math mode={b})".format(
            op=operator, b=band_math_mode))

    def add(self, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        return self._operator_binary("add", other, reverse=reverse)

    def subtract(self, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        return self._operator_binary("subtract", other, reverse=reverse)

    def divide(self, other: Union['DataCube', int, float]) -> 'DataCube':
        return self._operator_binary("divide", other)

    def multiply(self, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        return self._operator_binary("multiply", other, reverse=reverse)

    def normalized_difference(self, other: 'DataCube') -> 'DataCube':
        return self._operator_binary("normalized_difference", other)

    def logical_or(self, other: 'DataCube') -> 'DataCube':
        """
        Apply element-wise logical `or` operation
        :param other:
        :return DataCube: logical_or(this, other)
        """
        return self._operator_binary("or", other)

    def logical_and(self, other: 'DataCube') -> 'DataCube':
        """
        Apply element-wise logical `and` operation
        :param other:
        :return DataCube: logical_and(this, other)
        """
        return self._operator_binary("and", other)

    def __invert__(self) -> 'DataCube':
        return self._operator_unary("not")

    def __ne__(self, other: Union['DataCube', int, float]) -> 'DataCube':
        return self._operator_binary("neq", other)

    def __eq__(self, other: Union['DataCube', int, float]) -> 'DataCube':
        """
        Pixelwise comparison of this data cube with another cube or constant.

        :param other: Another data cube, or a constant
        :return:
        """
        return self._operator_binary("eq", other)

    def __gt__(self, other: Union['DataCube', int, float]) -> 'DataCube':
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.

        :param other:
        :return DataCube: this > other
        """
        return self._operator_binary("gt", other)

    def __lt__(self, other: Union['DataCube', int, float]) -> 'DataCube':
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.
        The number of bands in both data cubes has to be the same.

        :param other:
        :return DataCube: this < other
        """
        return self._operator_binary("lt", other)

    def __truediv__(self, other) -> 'DataCube':
        return self.divide(other)

    def __add__(self, other) -> 'DataCube':
        return self.add(other)

    def __radd__(self, other) -> 'DataCube':
        return self.add(other, reverse=True)

    def __sub__(self, other) -> 'DataCube':
        return self.subtract(other)

    def __rsub__(self, other) -> 'DataCube':
        return self.subtract(other, reverse=True)

    def __neg__(self) -> 'DataCube':
        return self.multiply(-1)

    def __mul__(self, other) -> 'DataCube':
        return self.multiply(other)

    def __rmul__(self, other) -> 'DataCube':
        return self.multiply(other, reverse=True)

    def __rpow__(self, other) -> 'DataCube':
        return self.power(other,reverse=True)

    def __pow__(self, other) -> 'DataCube':
        return self.power(other,reverse=False)

    def power(self,other,reverse):
        operator = "power"
        node = self._get_bandmath_node()
        x = {'from_node': node.reducer_process_graph()}
        y = other
        if reverse:
            x, y = y, x
        return self.process_with_node(node.clone_with_new_reducer(
            PGNode(operator, base=x, p=y)
        ))



    def __or__(self, other) -> 'DataCube':
        return self.logical_or(other)

    def __and__(self, other):
        return self.logical_and(other)

    def _bandmath_operator_binary_cubes(self, operator, other: 'DataCube',
                                        left_arg_name="x", right_arg_name="y"
                                        ) -> 'DataCube':
        """Band math binary operator with cube as right hand side argument"""
        left = self._get_bandmath_node()
        right = other._get_bandmath_node()
        if left.arguments["data"] != right.arguments["data"]:
            raise BandMathException("'Band math' between bands of different data cubes is not supported yet.")

        # Build reducer's sub-processgraph
        merged = PGNode(
            process_id=operator,
            arguments={
                left_arg_name: {"from_node": left.reducer_process_graph()},
                right_arg_name: {"from_node": right.reducer_process_graph()},
            }
        )
        return self.process_with_node(left.clone_with_new_reducer(merged))

    def _bandmath_operator_binary_scalar(self, operator: str, other: Union[int, float], reverse=False) -> 'DataCube':
        """Band math binary operator with scalar value (int or float) as right hand side argument"""
        node = self._get_bandmath_node()
        x = {'from_node': node.reducer_process_graph()}
        y = other
        if reverse:
            x, y = y, x
        return self.process_with_node(node.clone_with_new_reducer(
            PGNode(operator, x=x, y=y)
        ))

    def _bandmath_operator_unary(self, operator: str) -> 'DataCube':
        node = self._get_bandmath_node()
        return self.process_with_node(node.clone_with_new_reducer(
            PGNode(operator, x={'from_node': node.reducer_process_graph()})
        ))

    def _in_bandmath_mode(self) -> bool:
        # TODO #123 is it (still) necessary to make "band" math a special case?
        return isinstance(self._pg, ReduceNode) and self._pg.band_math_mode

    def _get_bandmath_node(self) -> ReduceNode:
        """Check we are in bandmath mode and return the node"""
        if not self._in_bandmath_mode():
            raise BandMathException("Must be in band math mode already")
        return self._pg

    def _merge_operator_binary_cubes(self, operator: str, other: 'DataCube', left_arg_name="x",
                                     right_arg_name="y") -> 'DataCube':
        """Merge two cubes with given operator as overlap_resolver."""
        # TODO #123 reuse an existing merge_cubes process graph if it already exists?
        return self.merge(other, overlap_resolver=PGNode(
            process_id=operator,
            arguments={
                left_arg_name: {"from_parameter": "cube1"},
                right_arg_name: {"from_parameter": "cube2"},
            }
        ))

    def zonal_statistics(self, regions, func, scale=1000, interval="day") -> 'DataCube':
        """
        Calculates statistics for each zone specified in a file.

        :param regions: GeoJSON or a path to a GeoJSON file containing the
                        regions. For paths you must specify the path to a
                        user-uploaded file without the user id in the path.
        :param func: Statistical function to calculate for the specified
                     zones. example values: min, max, mean, median, mode
        :param scale: A nominal scale in meters of the projection to work
                      in. Defaults to 1000.
        :param interval: Interval to group the time series. Allowed values:
                        day, wee, month, year. Defaults to day.

        :return: a DataCube instance
        """
        regions_geojson = regions
        if isinstance(regions, Polygon) or isinstance(regions, MultiPolygon):
            regions_geojson = mapping(regions)
        process_id = 'zonal_statistics'
        args = {
            'data': THIS,
            'regions': regions_geojson,
            'func': func,
            'scale': scale,
            'interval': interval
        }

        return self.process(process_id, args)

    def apply_dimension(
            self, code: str, runtime=None, version="latest", dimension='t', target_dimension=None
    ) -> 'DataCube':
        """
        Applies a user defined process to all pixel values along a dimension of a raster data cube. For example,
        if the temporal dimension is specified the process will work on a time series of pixel values.

        The process reduce_dimension also applies a process to pixel values along a dimension, but drops
        the dimension afterwards. The process apply applies a process to each pixel value in the data cube.

        The target dimension is the source dimension if not specified otherwise in the target_dimension parameter.
        The pixel values in the target dimension get replaced by the computed pixel values. The name, type and
        reference system are preserved.

        The dimension labels are preserved when the target dimension is the source dimension and the number of
        pixel values in the source dimension is equal to the number of values computed by the process. Otherwise,
        the dimension labels will be incrementing integers starting from zero, which can be changed using
        rename_labels afterwards. The number of labels will equal to the number of values computed by the process.

        :param code: UDF code or process identifier
        :param runtime: UDF runtime to use
        :param version: Version of the UDF runtime to use
        :param dimension: The name of the source dimension to apply the process on. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target_dimension: The name of the target dimension or null (the default) to use the source dimension
            specified in the parameter dimension. By specifying a target dimension, the source dimension is removed.
            The target dimension with the specified name and the type other (see add_dimension) is created, if it doesn't exist yet.

        :return: A datacube with the UDF applied to the given dimension.
        :raises: DimensionNotAvailable
        """
        if runtime:
            process = self._create_run_udf(code, runtime, version)
        else:
            process = PGNode(
                process_id=code,
                arguments={"data": {"from_parameter": "data"}},
            )
        arguments = {
            "data": self._pg,
            "process": PGNode.to_process_graph_argument(process),
            "dimension": self.metadata.assert_valid_dimension(dimension),
            # TODO #125 arguments: context
        }
        if target_dimension is not None:
            arguments["target_dimension"] = target_dimension
        return self.process_with_node(PGNode(
            process_id="apply_dimension",
            arguments=arguments
        ))

    def reduce_dimension(self, dimension: str, reducer: Union[PGNode, str],
                         process_id="reduce_dimension", band_math_mode: bool = False) -> 'DataCube':
        """
        Add a reduce process with given reducer callback along given dimension
        """
        # TODO: check if dimension is valid according to metadata? #116
        # TODO: #125 use/test case for `reduce_dimension_binary`?
        if isinstance(reducer, str):
            # Assume given reducer is a simple predefined reduce process_id
            reducer = PGNode(process_id=reducer, arguments={"data": {"from_parameter": "data"}})
        return self.process_with_node(ReduceNode(
            process_id=process_id,
            data=self._pg,
            reducer=reducer,
            dimension=self.metadata.assert_valid_dimension(dimension),
            # TODO #123 is it (still) necessary to make "band" math a special case?
            band_math_mode=band_math_mode
            # TODO: add `context` argument #125
        ), metadata=self.metadata.reduce_dimension(dimension_name=dimension))

    def _reduce_bands(self, reducer: PGNode) -> 'DataCube':
        # TODO #123 is it (still) necessary to make "band" math a special case?
        return self.reduce_dimension(dimension=self.metadata.band_dimension.name, reducer=reducer, band_math_mode=True)

    def _reduce_temporal(self, reducer: PGNode) -> 'DataCube':
        return self.reduce_dimension(dimension=self.metadata.temporal_dimension.name, reducer=reducer)

    def reduce_bands_udf(self, code: str, runtime="Python", version="latest") -> 'DataCube':
        """
        Apply reduce (`reduce_dimension`) process with given UDF along band/spectral dimension.
        """
        return self._reduce_bands(reducer=self._create_run_udf(code, runtime, version))

    def add_dimension(self, name: str, label: str, type: str = None):
        return self.process(
            process_id="add_dimension",
            arguments={"data": self._pg, "name": name, "label": label, "type": type},
            metadata=self.metadata.add_dimension(name=name, label=label, type=type)
        )

    def _create_run_udf(self, code, runtime, version) -> PGNode:
        # TODO: expose this publicly (or create dedicated PGNode subclass)? Also encapsulate/decouple UDF loading better?
        return PGNode(
            process_id="run_udf",
            arguments={
                "data": {
                    "from_parameter": "data"
                },
                "runtime": runtime,
                "version": version,
                "udf": code
            })

    def reduce_temporal_udf(self, code: str, runtime="Python", version="latest"):
        """
        Apply reduce (`reduce_dimension`) process with given UDF along temporal dimension.
        """
        return self._reduce_temporal(reducer=self._create_run_udf(code, runtime, version))

    @deprecated("use `reduce_temporal_udf` instead")
    def reduce_tiles_over_time(self, code: str, runtime="Python", version="latest"):
        """
        Applies a user defined function to a timeseries of tiles. The size of the tile is backend specific, and can be limited to one pixel.
        The function should reduce the given timeseries into a single (multiband) tile.

        :param code: The UDF code, compatible with the given runtime and version
        :param runtime: The UDF runtime
        :param version: The UDF runtime version
        :return:
        """
        return self.reduce_temporal_udf(code=code, runtime=runtime, version=version)

    def apply_neighborhood(self,process:PGNode, size:List[Dict],overlap:List[Dict]):
        """
        Applies a focal process to a data cube.

        A focal process is a process that works on a 'neighbourhood' of pixels. The neighbourhood can extend into multiple dimensions, this extent is specified by the `size` argument. It is not only (part of) the size of the input window, but also the size of the output for a given position of the sliding window. The sliding window moves with multiples of `size`.

        An overlap can be specified so that neighbourhoods can have overlapping boundaries. This allows for continuity of the output. The values included in the data cube as overlap can't be modified by the given `process`.

        The neighbourhood size should be kept small enough, to avoid running beyond computational resources, but a too small size will result in a larger number of process invocations, which may slow down processing. Window sizes for spatial dimensions typically are in the range of 64 to 512 pixels, while overlaps of 8 to 32 pixels are common.\n\nThe process must not add new dimensions, or remove entire dimensions, but the result can have different dimension labels.

        For the special case of 2D convolution, it is recommended to use ``apply_kernel()``.

        @param process:
        @param size:
        @param overlap:
        @return:
        """
        return self.process_with_node(PGNode(
            process_id='apply_neighborhood',
            arguments={
                "data": self._pg,
                "process": {"process_graph": process},
                "size": size,
                "overlap": overlap
            }
        ))

    def apply(self, process: Union[str, PGNode], data_argument='x') -> 'DataCube':
        if isinstance(process, str):
            # Simple single string process specification
            process = PGNode(
                process_id=process,
                arguments={data_argument: {"from_parameter": "x"}}
            )
        return self.process_with_node(PGNode(
            process_id='apply',
            arguments={
                "data": self._pg,
                "process": {"process_graph": process},
                # TODO #125 context
            }
        ))

    def reduce_temporal_simple(self, process_id="max") -> 'DataCube':
        """Do temporal reduce with a simple given process as callback."""
        return self._reduce_temporal(reducer=PGNode(
            process_id=process_id,
            arguments={"data": {"from_parameter": "data"}}
        ))

    def min_time(self) -> 'DataCube':
        """Finds the minimum value of a time series for all bands of the input dataset.

            :return: a DataCube instance
        """
        return self.reduce_temporal_simple("min")

    def max_time(self) -> 'DataCube':
        """
        Finds the maximum value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal_simple("max")

    def mean_time(self) -> 'DataCube':
        """Finds the mean value of a time series for all bands of the input dataset.

            :return: a DataCube instance
        """
        return self.reduce_temporal_simple("mean")

    def median_time(self) -> 'DataCube':
        """Finds the median value of a time series for all bands of the input dataset.

            :return: a DataCube instance
        """
        return self.reduce_temporal_simple("median")

    def count_time(self) -> 'DataCube':
        """Counts the number of images with a valid mask in a time series for all bands of the input dataset.

            :return: a DataCube instance
        """
        return self.reduce_temporal_simple("count")

    def ndvi(self, nir: str = None, red: str = None, target_band: str = None) -> 'DataCube':
        """ Normalized Difference Vegetation Index (NDVI)

            :param nir: (optional) name of NIR band
            :param red: (optional) name of red band
            :param target_band: (optional) name of the newly created band

            :return: a DataCube instance
        """
        if target_band is None:
            metadata = self.metadata.reduce_dimension(self.metadata.band_dimension.name)
        else:
            metadata = self.metadata.append_band(Band(target_band, "ndvi", None))
        return self.process(
            process_id='ndvi',
            arguments=dict_no_none(
                data=THIS,
                nir=nir, red=red, target_band=target_band
            ),
            metadata=metadata
        )

    def rename_dimension(self, source: str, target: str):
        """
        Renames a dimension in the data cube while preserving all other properties.

        :param source: The current name of the dimension. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target: A new Name for the dimension. Fails with a DimensionExists error if a dimension with the specified name exists.

        :return: A new datacube with the dimension renamed.
        """
        if target in self.metadata.dimension_names():
            raise ValueError('Target dimension name conflicts with existing dimension: %s.' % target)
        return self.process(
            process_id='rename_dimension',
            arguments=dict_no_none(
                data=THIS,
                source=self.metadata.assert_valid_dimension(source),
                target=target
            )
        )

    def rename_labels(self, dimension: str, target: list, source: list = None) -> 'DataCube':
        """ Renames the labels of the specified dimension in the data cube from source to target.

            :param dimension: Dimension name
            :param target: The new names for the labels.
            :param source: The names of the labels as they are currently in the data cube.

            :return: An DataCube instance
        """
        return self.process(
            process_id='rename_labels',
            arguments=dict_no_none(
                data=THIS,
                dimension=self.metadata.assert_valid_dimension(dimension),
                target=target,
                source=source
            )
        )

    @deprecated("use 'linear_scale_range' instead")
    def stretch_colors(self, min, max) -> 'DataCube':
        """ Color stretching
        deprecated, use 'linear_scale_range' instead

            :param min: Minimum value
            :param max: Maximum value
            :return: a DataCube instance
        """
        process_id = 'stretch_colors'
        args = {
            'data': THIS,
            'min': min,
            'max': max
        }

        return self.process(process_id, args)

    def linear_scale_range(self, input_min, input_max, output_min, output_max) -> 'DataCube':
        """ Color stretching
            :param input_min: Minimum input value
            :param input_max: Maximum input value
            :param output_min: Minimum output value
            :param output_max: Maximum output value
            :return a DataCube instance
        """
        process_id = 'linear_scale_range'
        args = {
            'x': THIS,
            'inputMin': input_min,
            'inputMax': input_max,
            'outputMin': output_min,
            'outputMax': output_max
        }
        return self.process(process_id, args)

    def mask(self, mask: 'DataCube' = None, replacement=None) -> 'DataCube':
        """
        Applies a mask to a raster data cube. To apply a vector mask use `mask_polygon`.

        A mask is a raster data cube for which corresponding pixels among `data` and `mask`
        are compared and those pixels in `data` are replaced whose pixels in `mask` are non-zero
        (for numbers) or true (for boolean values).
        The pixel values are replaced with the value specified for `replacement`,
        which defaults to null (no data).

        :param mask: the raster mask
        :param replacement: the value to replace the masked pixels with
        """
        return self.process(
            process_id="mask",
            arguments=dict_no_none(
                data=THIS,
                mask={'from_node': mask._pg},
                replacement=replacement
            )
        )

    def mask_polygon(
            self, mask: Union[Polygon, MultiPolygon, str, pathlib.Path] = None,
            srs="EPSG:4326", replacement=None, inside: bool = None
    ) -> 'DataCube':
        """
        Applies a polygon mask to a raster data cube. To apply a raster mask use `mask`.

        All pixels for which the point at the pixel center does not intersect with any
        polygon (as defined in the Simple Features standard by the OGC) are replaced.
        This behaviour can be inverted by setting the parameter `inside` to true.

        The pixel values are replaced with the value specified for `replacement`,
        which defaults to `no data`.

        :param mask: A polygon, provided as a :class:`shapely.geometry.Polygon` or :class:`shapely.geometry.MultiPolygon`, or a filename pointing to a valid vector file
        :param srs: The reference system of the provided polygon, by default this is Lat Lon (EPSG:4326).
        :param replacement: the value to replace the masked pixels with
        """
        if isinstance(mask, (str, pathlib.Path)):
            # TODO: default to loading file client side?
            # TODO: change read_vector to load_uploaded_files https://github.com/Open-EO/openeo-processes/pull/106
            read_vector = self.process(
                process_id='read_vector',
                arguments={'filename': str(mask)}
            )
            mask = {'from_node': read_vector._pg}
        elif isinstance(mask, shapely.geometry.base.BaseGeometry):
            if mask.area == 0:
                raise ValueError("Mask {m!s} has an area of {a!r}".format(m=mask, a=mask.area))
            mask = shapely.geometry.mapping(mask)
            mask['crs'] = {
                'type': 'name',
                'properties': {'name': srs}
            }
        else:
            # Assume mask is already a valid GeoJSON object
            assert "type" in mask

        return self.process(
            process_id="mask_polygon",
            arguments=dict_no_none(
                data=THIS,
                mask=mask,
                replacement=replacement,
                inside=inside
            )
        )

    def merge(self, other: 'DataCube', overlap_resolver: Union[str, PGNode] = None) -> 'DataCube':
        arguments = {
            'cube1': {'from_node': self._pg},
            'cube2': {'from_node': other._pg},
        }
        if overlap_resolver:
            if isinstance(overlap_resolver, str):
                # Simple resolver (specified as process_id string)
                overlap_resolver = PGNode(
                    process_id=overlap_resolver,
                    arguments={"data": [{"from_parameter": "x"}, {"from_parameter": "y"}]}
                )
            arguments["overlap_resolver"] = {"process_graph": overlap_resolver}
        # TODO #125 context
        # TODO: set metadata of reduced cube?
        return self.process_with_node(PGNode(process_id="merge_cubes", arguments=arguments))

    def apply_kernel(self, kernel: Union[np.ndarray, List[List[float]]], factor=1.0) -> 'DataCube':
        """
        Applies a focal operation based on a weighted kernel to each value of the specified dimensions in the data cube.

        :param kernel: The kernel to be applied on the data cube. It should be a 2D (numpy) array.
        :param factor: A factor that is multiplied to each value computed by the focal operation. This is basically a shortcut for explicitly multiplying each value by a factor afterwards, which is often required for some kernel-based algorithms such as the Gaussian blur.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        return self.process('apply_kernel', {
            'data': THIS,
            'kernel': kernel.tolist() if isinstance(kernel, np.ndarray) else kernel,
            'factor': factor
        })

    def raster_to_vector(self) -> 'VectorCube':
        """
        EXPERIMENTAL: not generally supported, API subject to change
        Converts this raster data cube into a vector data cube. The bounding polygon of homogenous areas of pixels is constructed.


        @return: A vectorcube
        """
        return VectorCube(PGNode(
            process_id='raster_to_vector',
            arguments={
                'data': self._pg
            }),connection=self._connection, metadata=self.metadata)

    ####VIEW methods #######

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a mean time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """

        return self._polygonal_timeseries(polygon, "mean")

    def polygonal_histogram_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a histogram time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """

        return self._polygonal_timeseries(polygon, "histogram")

    def polygonal_median_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a median time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """

        return self._polygonal_timeseries(polygon, "median")

    def polygonal_standarddeviation_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a time series of standard deviations for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """

        return self._polygonal_timeseries(polygon, "sd")

    def _polygonal_timeseries(self, polygon: Union[Polygon, MultiPolygon, str], func: str) -> 'DataCube':

        if isinstance(polygon, str):
            # polygon is a path to vector file
            # TODO this is non-standard process: check capabilities? #104 #40
            geometries = PGNode(process_id="read_vector", arguments={"filename": polygon})
        else:
            geometries = shapely.geometry.mapping(polygon)
            geometries['crs'] = {
                'type': 'name',  # TODO: name?
                'properties': {
                    'name': 'EPSG:4326'
                }
            }

        return self.process_with_node(PGNode(
            process_id="aggregate_spatial",
            arguments={
                "data": self._pg,
                "geometries": geometries,
                "reducer": {"process_graph": PGNode(
                    process_id=func,
                    arguments={"data": {"from_parameter": "data"}}
                )},
                # TODO #125 target dimension, context
            }
        ))

    def save_result(self, format: str = "GTiff", options: dict = None):
        formats = set(self._connection.list_output_formats().keys())
        if format.lower() not in {f.lower() for f in formats}:
            raise ValueError("Invalid format {f!r}. Should be one of {s}".format(f=format, s=formats))
        return self.process(
            process_id="save_result",
            arguments={
                "data": THIS,
                "format": format,
                "options": options or {}
            }
        )

    def download(self, outputfile: str, format: str = "GTIFF", options: dict = None):
        """Download image collection, e.g. as GeoTIFF."""
        newcollection = self.save_result(format=format, options=options)
        return self._connection.download(newcollection._pg.flatten(), outputfile)

    def tiled_viewing_service(self, type: str, **kwargs) -> Dict:
        return self._connection.create_service(self._pg.flatten(), type=type, **kwargs)

    def execute_batch(
            self,
            outputfile: Union[str, pathlib.Path], out_format: str = None,
            print=print, max_poll_interval=60, connection_retry_interval=30,
            job_options=None, **format_options) -> RESTJob:
        """
        Evaluate the process graph by creating a batch job, and retrieving the results when it is finished.
        This method is mostly recommended if the batch job is expected to run in a reasonable amount of time.

        For very long running jobs, you probably do not want to keep the client running.

        :param job_options:
        :param outputfile: The path of a file to which a result can be written
        :param out_format: (optional) Format of the job result.
        :param format_options: String Parameters for the job result format

        """
        job = self.send_job(out_format, job_options=job_options, **format_options)
        return job.run_synchronous(
            # TODO #135 support multi file result sets too
            outputfile=outputfile,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )

    def send_job(self, out_format=None, job_options=None, **format_options) -> RESTJob:
        """
        Sends a job to the backend and returns a Job instance. The job will still need to be started and managed explicitly.
        The :func:`~openeo.imagecollection.ImageCollection.execute_batch` method allows you to run batch jobs without managing it.

        :param out_format: String Format of the job result.
        :param job_options: A dictionary containing (custom) job options
        :param format_options: String Parameters for the job result format
        :return: status: Job resulting job.
        """
        img = self
        if out_format:
            # add `save_result` node
            img = img.save_result(format=out_format, options=format_options)
        return self._connection.create_job(process_graph=img.graph, additional=job_options)

    def save_user_defined_process(self, user_defined_process_id: str, public: bool = False) -> RESTUserDefinedProcess:
        """
        Saves this process graph in the backend as a user-defined process for the authenticated user.

        :param user_defined_process_id: unique identifier for the process
        :param public: visible to other users?
        :return: a RESTUserDefinedProcess instance
        """
        return self._connection.save_user_defined_process(user_defined_process_id=user_defined_process_id,
                                                          process_graph=self.graph, public=public)

    def execute(self) -> Dict:
        """Executes the process graph of the imagery. """
        return self._connection.execute(self._pg.flatten())

    def to_graphviz(self):
        """
        Build a graphviz DiGraph from the process graph
        :return:
        """
        # pylint: disable=import-error, import-outside-toplevel
        import graphviz
        import pprint

        graph = graphviz.Digraph(node_attr={"shape": "none", "fontname": "sans", "fontsize": "11"})
        for name, process in self.graph.items():
            args = process.get("arguments", {})
            # Build label
            label = '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">'
            label += '<TR><TD COLSPAN="2" BGCOLOR="#eeeeee">{pid}</TD></TR>'.format(pid=process.get("process_id","unknown"))
            label += "".join(
                '''<TR><TD ALIGN="RIGHT">{arg}</TD>
                       <TD ALIGN="LEFT"><FONT FACE="monospace">{value}</FONT></TD></TR>'''.format(
                    arg=k, value=pprint.pformat(v)[:1000].replace('\n', '<BR/>')
                ) for k, v in sorted(args.items())
            )
            label += '</TABLE>>'
            # Add node and edges to graph
            graph.node(name, label=label)
            for arg in args.values():
                if isinstance(arg, dict) and "from_node" in arg:
                    graph.edge(arg["from_node"], name)

            # TODO: add subgraph for "reducer" arguments?

        return graph

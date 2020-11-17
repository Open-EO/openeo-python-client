"""
The main module for creating earth observation processes. It aims to easily build complex process chains, that can
be evaluated by an openEO backend.

.. data:: THIS

    Symbolic reference to the current data cube, to be used as argument in DataCube.process() calls

"""
import datetime
import inspect
import json
import logging
import pathlib
import typing
from builtins import staticmethod
from typing import List, Dict, Union, Tuple

import numpy
import numpy as np
import shapely.geometry
import shapely.geometry.base
from deprecated import deprecated
from shapely.geometry import Polygon, MultiPolygon, mapping

import openeo
import openeo.processes
from openeo.imagecollection import ImageCollection, CollectionMetadata
from openeo.internal.graph_building import PGNode, ReduceNode
from openeo.metadata import Band
from openeo.processes import ProcessBuilder
from openeo.rest import BandMathException, OperatorException, OpenEoClientException
from openeo.rest.job import RESTJob
from openeo.rest.udp import RESTUserDefinedProcess
from openeo.util import get_temporal_extent, dict_no_none, legacy_alias
from openeo.vectorcube import VectorCube
from openeo.rest.result import Result

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

    def __init__(self, graph: PGNode, connection: 'openeo.Connection', metadata: CollectionMetadata = None):
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

    def to_json(self, indent=2, separators=None) -> str:
        """
        Get JSON representation of (flattened) process graph.
        """
        return json.dumps(self.flatten(), indent=indent, separators=separators)

    @property
    def _api_version(self):
        return self._connection.capabilities().api_version_check

    @property
    def connection(self) -> 'openeo.Connection':
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

    graph_add_node = legacy_alias(process, "graph_add_node")

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
            cls, collection_id: str, connection: 'openeo.Connection' = None,
            spatial_extent: Union[Dict[str, float], None] = None,
            temporal_extent: Union[List[Union[str, datetime.datetime, datetime.date]], None] = None,
            bands: Union[List[str], None] = None,
            fetch_metadata=True,
            properties: Dict[str, Union[str, PGNode, typing.Callable]] = None
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
                prop: cls._get_callback(pred, parent_parameters=["value"])
                for prop, pred in properties.items()
            }
        pg = PGNode(
            process_id='load_collection',
            arguments=arguments
        )
        if bands:
            metadata = metadata.filter_bands(bands)
        return cls(graph=pg, connection=connection, metadata=metadata)

    create_collection = legacy_alias(load_collection, name="create_collection")

    @classmethod
    def load_disk_collection(cls, connection: 'openeo.Connection', file_format: str, glob_pattern: str,
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
        return cls(graph=pg, connection=connection, metadata=CollectionMetadata({}))

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

    band_filter = legacy_alias(filter_bands, "band_filter")

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

    def __ge__(self, other: Union['DataCube', int, float]) -> 'DataCube':
        return self._operator_binary("gte", other)

    def __lt__(self, other: Union['DataCube', int, float]) -> 'DataCube':
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.
        The number of bands in both data cubes has to be the same.

        :param other:
        :return DataCube: this < other
        """
        return self._operator_binary("lt", other)

    def __le__(self, other: Union['DataCube', int, float]) -> 'DataCube':
        return self._operator_binary("lte", other)

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

    def __truediv__(self, other) -> 'DataCube':
        return self.divide(other)

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
        return self.merge_cubes(other, overlap_resolver=PGNode(
            process_id=operator,
            arguments={
                left_arg_name: {"from_parameter": "x"},
                right_arg_name: {"from_parameter": "y"},
            }
        ))

    def aggregate_spatial(
            self,
            geometries: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path],
            reducer: Union[str, PGNode, typing.Callable]
    ) -> 'DataCube':
        """
        Aggregates statistics for one or more geometries (e.g. zonal statistics for polygons)
        over the spatial dimensions.

        :param geometries: shapely geometry, GeoJSON dictionary or path to GeoJSON file
        :param reducer: a callback function that creates a process graph, see :ref:`callbackfunctions`
        :return:
        """
        # TODO #125 arguments: target dimension, context
        if isinstance(geometries, (str, pathlib.Path)):
            # polygon is a path to vector file
            # TODO this is non-standard process: check capabilities? #104 #40. Load polygon client side otherwise
            geometries = PGNode(process_id="read_vector", arguments={"filename": str(geometries)})
        elif isinstance(geometries, shapely.geometry.base.BaseGeometry):
            geometries = mapping(geometries)
        elif isinstance(geometries, dict) and geometries["type"] in (
                # TODO: support more geometry types?
                "Polygon", "MultiPolygon", "GeometryCollection"
        ):
            pass
        else:
            raise OpenEoClientException("Invalid `geometries`: {g!r}".format(g=geometries))
        reducer = self._get_callback(reducer, parent_parameters=["data"])
        return self.process(process_id="aggregate_spatial", data=THIS, geometries=geometries, reducer=reducer)

    @deprecated(reason="use aggregate_spatial instead")
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

    @staticmethod
    def _get_callback(process: Union[str, PGNode, typing.Callable], parent_parameters: List[str]) -> dict:
        """
        Build a "callback" process: a user defined process that is used by another process (such
        as `apply`, `apply_dimension`, `reduce`, ....)

        :param process: process id string, PGNode or callable that uses the ProcessBuilder mechanism to build a process
        :parameter parameter_mapping: mapping of child (callback) parameters names to parent process parameter names
        :return:
        """

        def get_parameter_names(process: typing.Callable) -> List[str]:
            signature = inspect.signature(process)
            return [
                p.name for p in signature.parameters.values()
                if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]

        # TODO: autodetect the parameters defined by process?
        if isinstance(process, PGNode):
            # Assume this is already a valid callback process
            pg = process
        elif isinstance(process, str):
            # Assume given reducer is a simple predefined reduce process_id
            if process in openeo.processes.__dict__:
                process_params = get_parameter_names(openeo.processes.__dict__[process])
            else:
                # Best effort guess
                process_params = parent_parameters
            if parent_parameters == ["x", "y"] and (len(process_params) == 1 or process_params[:1] == ["data"]):
                # Special case: wrap all parent parameters in an array
                arguments = {process_params[0]: [{"from_parameter": p} for p in parent_parameters]}
            else:
                arguments = {a: {"from_parameter": b} for a, b in zip(process_params, parent_parameters)}
            pg = PGNode(process_id=process, arguments=arguments)
        elif isinstance(process, typing.Callable):
            process_params = get_parameter_names(process)
            if parent_parameters == ["x", "y"] and (len(process_params) == 1 or process_params[:1] == ["data"]):
                # Special case: wrap all parent parameters in an array
                arguments = [ProcessBuilder([{"from_parameter": p} for p in parent_parameters])]
            else:
                arguments = [ProcessBuilder({"from_parameter": p}) for p in parent_parameters]

            pg = process(*arguments).pgnode
        else:
            raise ValueError(process)

        return PGNode.to_process_graph_argument(pg)

    def apply_dimension(
            self, code: str = None, runtime=None,
            process: [str, PGNode, typing.Callable] = None,
            version="latest", dimension='t', target_dimension=None
    ) -> 'DataCube':
        """
        Applies a process to all pixel values along a dimension of a raster data cube. For example,
        if the temporal dimension is specified the process will work on a time series of pixel values.

        The process to apply is specified by either `code` and `runtime` in case of a UDF, or by providing a callback function
        in the `process` argument.

        The process reduce_dimension also applies a process to pixel values along a dimension, but drops
        the dimension afterwards. The process apply applies a process to each pixel value in the data cube.

        The target dimension is the source dimension if not specified otherwise in the target_dimension parameter.
        The pixel values in the target dimension get replaced by the computed pixel values. The name, type and
        reference system are preserved.

        The dimension labels are preserved when the target dimension is the source dimension and the number of
        pixel values in the source dimension is equal to the number of values computed by the process. Otherwise,
        the dimension labels will be incrementing integers starting from zero, which can be changed using
        rename_labels afterwards. The number of labels will equal to the number of values computed by the process.

        :param code: UDF code or process identifier (optional)
        :param runtime: UDF runtime to use (optional)
        :param process: a callback function that creates a process graph, see :ref:`callbackfunctions`
        :param version: Version of the UDF runtime to use
        :param dimension: The name of the source dimension to apply the process on. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target_dimension: The name of the target dimension or null (the default) to use the source dimension
            specified in the parameter dimension. By specifying a target dimension, the source dimension is removed.
            The target dimension with the specified name and the type other (see add_dimension) is created, if it doesn't exist yet.

        :return: A datacube with the UDF applied to the given dimension.
        :raises: DimensionNotAvailable
        """
        if runtime:
            # TODO EP-3555: unify better with UDF(PGNode) class and avoid doing same UDF code-runtime-version argument stuff in each method
            callback_process_node = self._create_run_udf(code, runtime, version)
            process = PGNode.to_process_graph_argument(callback_process_node)
        elif code or process:
            # TODO EP-3555 unify `code` and `process`
            process = self._get_callback(code or process, parent_parameters=["data"])
        else:
            raise OpenEoClientException("No UDF code or process given")
        arguments = {
            "data": THIS,
            "process": process,
            "dimension": self.metadata.assert_valid_dimension(dimension),
            # TODO #125 arguments: context
        }
        if target_dimension is not None:
            arguments["target_dimension"] = target_dimension
        result_cube = self.process(process_id="apply_dimension", arguments=arguments)

        return result_cube

    def reduce_dimension(
            self, dimension: str, reducer: Union[str, PGNode, typing.Callable],
            process_id="reduce_dimension", band_math_mode: bool = False
    ) -> 'DataCube':
        """
        Add a reduce process with given reducer callback along given dimension

        :param dimension: the label of the dimension to reduce
        :param reducer: a callback function that creates a process graph, see :ref:`callbackfunctions`
        """
        # TODO: check if dimension is valid according to metadata? #116
        # TODO: #125 use/test case for `reduce_dimension_binary`?
        reducer = self._get_callback(reducer, parent_parameters=["data"])

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
        # TODO EP-3555: unify better with UDF(PGNode) class and avoid doing same UDF code-runtime-version argument stuff in each method
        return self._reduce_bands(reducer=self._create_run_udf(code, runtime, version))

    def add_dimension(self, name: str, label: str, type: str = None):
        """
        Adds a new named dimension to the data cube.
        Afterwards, the dimension can be referenced with the specified name. If a dimension with the specified name exists,
        the process fails with a DimensionExists error. The dimension label of the dimension is set to the specified label.

        This call does not modify the datacube in place, but returns a new datacube with the additional dimension.

        @param name: The name of the dimension to add
        @param label: The dimension label.
        @param type: Dimension type, allowed values: 'spatial', 'temporal', 'bands', 'other', default value is 'other'
        @return: The data cube with a newly added dimension. The new dimension has exactly one dimension label. All other dimensions remain unchanged.
        """
        return self.process(
            process_id="add_dimension",
            arguments={"data": self._pg, "name": name, "label": label, "type": type},
            metadata=self.metadata.add_dimension(name=name, label=label, type=type)
        )

    def _create_run_udf(self, code, runtime, version) -> PGNode:
        # TODO EP-3555: unify better with UDF(PGNode) class
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

        :param code: The UDF code, compatible with the given runtime and version
        :param runtime: The UDF runtime
        :param version: The UDF runtime version
        """
        # TODO EP-3555: unify better with UDF(PGNode) class and avoid doing same UDF code-runtime-version argument stuff in each method
        return self._reduce_temporal(reducer=self._create_run_udf(code, runtime, version))

    reduce_tiles_over_time = legacy_alias(reduce_temporal_udf, name="reduce_tiles_over_time")

    def apply_neighborhood(
            self, process: [str, PGNode, typing.Callable],
            size: List[Dict], overlap: List[dict] = None
    ) -> 'DataCube':
        """
        Applies a focal process to a data cube.

        A focal process is a process that works on a 'neighbourhood' of pixels. The neighbourhood can extend into multiple dimensions, this extent is specified by the `size` argument. It is not only (part of) the size of the input window, but also the size of the output for a given position of the sliding window. The sliding window moves with multiples of `size`.

        An overlap can be specified so that neighbourhoods can have overlapping boundaries. This allows for continuity of the output. The values included in the data cube as overlap can't be modified by the given `process`.

        The neighbourhood size should be kept small enough, to avoid running beyond computational resources, but a too small size will result in a larger number of process invocations, which may slow down processing. Window sizes for spatial dimensions typically are in the range of 64 to 512 pixels, while overlaps of 8 to 32 pixels are common.\n\nThe process must not add new dimensions, or remove entire dimensions, but the result can have different dimension labels.

        For the special case of 2D convolution, it is recommended to use ``apply_kernel()``.

        :param size:
        :param overlap:
        :param process: a callback function that creates a process graph, see :ref:`callbackfunctions`
        :return:
        """
        return self.process(
            process_id='apply_neighborhood',
            arguments=dict_no_none(
                data=THIS,
                process=self._get_callback(process, parent_parameters=["data"]),
                size=size,
                overlap=overlap
            )
        )

    def apply(self, process: Union[str, PGNode, typing.Callable] = None) -> 'DataCube':
        """
        Applies a unary process (a local operation) to each value of the specified or all dimensions in the data cube.

        :param process: the name of a process, or a callback function that creates a process graph, see :ref:`callbackfunctions`
        :param dimensions: The names of the dimensions to apply the process on. Defaults to an empty array so that all dimensions are used.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        return self.process(
            process_id="apply",
            arguments={
                "data": THIS,
                "process": self._get_callback(process, parent_parameters=["x"]),
                # TODO #125 context
            }
        )

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

    def aggregate_temporal(self, intervals:List[List],reducer: Union[str, PGNode, typing.Callable],labels:List = None, dimension:str = None, context:Dict=None) -> 'DataCube' :
        """ Computes a temporal aggregation based on an array of date and/or time intervals.

            Calendar hierarchies such as year, month, week etc. must be transformed into specific intervals by the clients. For each interval, all data along the dimension will be passed through the reducer. The computed values will be projected to the labels, so the number of labels and the number of intervals need to be equal.

            If the dimension is not set, the data cube is expected to only have one temporal dimension.

            :param intervals: Temporal left-closed intervals so that the start time is contained, but not the end time.
            :param reducer: A reducer to be applied on all values along the specified dimension. The reducer must be a callable process (or a set processes) that accepts an array and computes a single return value of the same type as the input values, for example median.
            :param labels: Labels for the intervals. The number of labels and the number of groups need to be equal.
            :param dimension: The temporal dimension for aggregation. All data along the dimension will be passed through the specified reducer. If the dimension is not set, the data cube is expected to only have one temporal dimension.
            :param context: Additional data to be passed to the reducer. Not set by default.

            :return: An ImageCollection containing  a result for each time window
        """
        return self.process(
            process_id="aggregate_temporal",
            arguments=dict_no_none(
                data= THIS,
                intervals = intervals,
                labels = labels,
                dimension = dimension,
                reducer = self._get_callback(reducer, parent_parameters=["data"])

            )
        )

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
            ),
            metadata=self.metadata.rename_dimension(source,target)
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
            ),
            metadata=self.metadata.rename_labels(dimension,target,source)
        )

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

    def merge_cubes(
            self, other: 'DataCube', overlap_resolver: Union[str, PGNode, typing.Callable] = None
    ) -> 'DataCube':
        """
        Merging two data cubes

        The data cubes have to be compatible. A merge operation without overlap should be reversible with (a set of) filter operations for each of the two cubes. The process performs the join on overlapping dimensions, with the same name and type.
        An overlapping dimension has the same name, type, reference system and resolution in both dimensions, but can have different labels. One of the dimensions can have different labels, for all other dimensions the labels must be equal. If data overlaps, the parameter overlap_resolver must be specified to resolve the overlap.

        Examples for merging two data cubes:

        #. Data cubes with the dimensions x, y, t and bands have the same dimension labels in x,y and t, but the labels for the dimension bands are B1 and B2 for the first cube and B3 and B4. An overlap resolver is not needed. The merged data cube has the dimensions x, y, t and bands and the dimension bands has four dimension labels: B1, B2, B3, B4.
        #. Data cubes with the dimensions x, y, t and bands have the same dimension labels in x,y and t, but the labels for the dimension bands are B1 and B2 for the first data cube and B2 and B3 for the second. An overlap resolver is required to resolve overlap in band B2. The merged data cube has the dimensions x, y, t and bands and the dimension bands has three dimension labels: B1, B2, B3.
        #. Data cubes with the dimensions x, y and t have the same dimension labels in x,y and t. There are two options:
                * Keep the overlapping values separately in the merged data cube: An overlap resolver is not needed, but for each data cube you need to add a new dimension using add_dimension. The new dimensions must be equal, except that the labels for the new dimensions must differ by name. The merged data cube has the same dimensions and labels as the original data cubes, plus the dimension added with add_dimension, which has the two dimension labels after the merge.
                * Combine the overlapping values into a single value: An overlap resolver is required to resolve the overlap for all pixels. The merged data cube has the same dimensions and labels as the original data cubes, but all pixel values have been processed by the overlap resolver.
        #. Merging a data cube with dimensions x, y, t with another cube with dimensions x, y will join on the x, y dimension, so the lower dimension cube is merged with each time step in the higher dimensional cube. This can for instance be used to apply a digital elevation model to a spatiotemporal data cube.

        @param other: The data cube to merge with.
        @param overlap_resolver: A reduction operator that resolves the conflict if the data overlaps. The reducer must return a value of the same data type as the input values are. The reduction operator may be a single process such as multiply or consist of multiple sub-processes. null (the default) can be specified if no overlap resolver is required.
        @return: The merged data cube.
        """
        arguments = {
            'cube1': {'from_node': self._pg},
            'cube2': {'from_node': other._pg},
        }
        if overlap_resolver:
            arguments["overlap_resolver"] = self._get_callback(overlap_resolver, parent_parameters=["x", "y"])
        # TODO #125 context
        # TODO: set metadata of reduced cube?
        return self.process(process_id="merge_cubes", arguments=arguments)

    merge = legacy_alias(merge_cubes, name="merge")

    def apply_kernel(self, kernel: Union[np.ndarray, List[List[float]]], factor=1.0, border = 0, replace_invalid=0) -> 'DataCube':
        """
        Applies a focal operation based on a weighted kernel to each value of the specified dimensions in the data cube.

        The border parameter determines how the data is extended when the kernel overlaps with the borders.
        The following options are available:

        * numeric value - fill with a user-defined constant number n: nnnnnn|abcdefgh|nnnnnn (default, with n = 0)
        * replicate - repeat the value from the pixel at the border: aaaaaa|abcdefgh|hhhhhh
        * reflect - mirror/reflect from the border: fedcba|abcdefgh|hgfedc
        * reflect_pixel - mirror/reflect from the center of the pixel at the border: gfedcb|abcdefgh|gfedcb
        * wrap - repeat/wrap the image: cdefgh|abcdefgh|abcdef


        :param kernel: The kernel to be applied on the data cube. The kernel has to be as many dimensions as the data cube has dimensions.
        :param factor: A factor that is multiplied to each value computed by the focal operation. This is basically a shortcut for explicitly multiplying each value by a factor afterwards, which is often required for some kernel-based algorithms such as the Gaussian blur.
        :param border: Determines how the data is extended when the kernel overlaps with the borders. Defaults to fill the border with zeroes.
        :param: replace_invalid: This parameter specifies the value to replace non-numerical or infinite numerical values with. By default, those values are replaced with zeroes.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        return self.process('apply_kernel', {
            'data': THIS,
            'kernel': kernel.tolist() if isinstance(kernel, np.ndarray) else kernel,
            'factor': factor,
            'border': border,
            'replace_invalid': replace_invalid
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

    def _polygonal_timeseries(
            self, polygon: Union[Polygon, MultiPolygon, str], func: Union[str, PGNode, typing.Callable]
    ) -> 'DataCube':
        return self.aggregate_spatial(geometries=polygon, reducer=func)

    def atmospheric_correction(self,method=None):
        """
        EXPERIMENTAL
        Applies an atmospheric correction that converts top of atmosphere reflectance values into bottom of atmosphere/top of canopy reflectance values.

        Note that multiple atmospheric methods exist, but may not be supported by all backends. The method parameter gives
        you the option of requiring a specific method, but this may result in an error if the backend does not support it.

        @return: datacube with bottom of atmosphere reflectances
        """
        return self.process('atmospheric_correction', {
            'data': THIS,
            'method': method
        })

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

    def download(self, outputfile: Union[str, pathlib.Path, None] = None, format: str = "GTIFF", options: dict = None):
        """
        Download image collection, e.g. as GeoTIFF.
        If :param: outputfile is provided, the result is stored on disk locally, otherwise, a bytes object is returned.
        The bytes object can be passed on to a suitable decoder for decoding.

        :param outputfile: Optional, an output file if the result needs to be stored on disk.
        :param format: Optional, defaults to "GTIFF", an output format supported by the backend.
        :param options: Optional, file format options
        :return: None if the result is stored to disk, or a bytes object returned by the backend.
        """
        newcollection = self.save_result(format=format, options=options)
        return self._connection.download(newcollection._pg.flatten(), outputfile)

    def tiled_viewing_service(self, type: str, **kwargs) -> Dict:
        return self._connection.create_service(self._pg.flatten(), type=type, **kwargs)

    def execute_batch(
            self,
            outputfile: Union[str, pathlib.Path] = None, out_format: str = None,
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
        job = job.run_synchronous(
            outputfile=None,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )
        # relying on RestJOB::start_and_wait called in run_synchronous throws if job is not finished properly
        # TODO: avoid calling private methods from openeo.rest.Result
        result = job.get_result()
        assets = result._get_assets();
        if (len(assets)==1):
            result._download_url(assets.popitem()[1]["href"], pathlib.Path(outputfile))
        else:
            # TODO: find a mechanism that works accross the backends that can reliable choose "primary" result
            log.warning("FIXME: Multiple result files detected, those will be saved by the name advertised on the server!")
            for iname,iurl in assets.items():
                result._download_url(iurl["href"], pathlib.Path(iname))
                
        return job

    def send_job(
            self, out_format=None, title: str = None, description: str = None, plan: str = None, budget=None,
            job_options=None, **format_options
    ) -> RESTJob:
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
        return self._connection.create_job(
            process_graph=img.graph,
            title=title, description=description, plan=plan, budget=budget, additional=job_options
        )

    def save_user_defined_process(self, user_defined_process_id: str, public: bool = False, summary:str=None, description:str=None) -> RESTUserDefinedProcess:
        """
        Saves this process graph in the backend as a user-defined process for the authenticated user.

        :param user_defined_process_id: unique identifier for the process
        :param public: visible to other users?
        :param summary: A short summary of what the process does.
        :param description: Detailed description to explain the entity. CommonMark 0.29 syntax MAY be used for rich text representation.
        :return: a RESTUserDefinedProcess instance
        """
        return self._connection.save_user_defined_process(user_defined_process_id=user_defined_process_id,
                                                          process_graph=self.graph, public=public, summary=summary, description=description)

    def execute(self) -> Dict:
        """Executes the process graph of the imagery. """
        return self._connection.execute(self._pg.flatten())

    @staticmethod
    def execute_local_udf(udf: str, datacube: Union[str, 'xarray.DataArray', 'openeo_udf.api.datacube.DataCube'] =None , fmt='netcdf'):
        """
        Locally executes an user defined function on a previously downloaded datacube.
        
        :param udf: the code of the user defined function
        :param datacube: the path to the downloaded data in disk or a DataCube
        :param fmt: format of the file if datacube is string
        :return: the resulting DataCube
        """
        from openeo_udf.api.udf_data import UdfData
        from openeo_udf.api.run_code import run_user_code
        from openeo_udf.api.datacube import DataCube as udf_DataCube
        from xarray import DataArray
        
        udf_data=None
        # if it is a datacube
        if datacube is not None:
            # get input
            if isinstance(datacube, str):
                from openeo.rest.conversions import datacube_from_file
                d=datacube_from_file(datacube, fmt)
            elif isinstance(datacube, udf_DataCube):
                d=datacube
            elif isinstance(datacube, DataArray):
                d=udf_DataCube(datacube)
            else:
                raise TypeError("Data should be either file name to the Data or a DataCube, got "+str(datacube.__class__ if datacube is not None else 'None'))
            # datacube's data is to be float and x,y not provided
            d=udf_DataCube(d.get_array()
                .astype(numpy.float64)
                .drop(labels='x')
                .drop(labels='y')
            )
            # wrap to udf_data
            udf_data=UdfData(datacube_list=[d])
            
        # TODO: enrich to other types like time series, vector data,... probalby by adding  named arguments
        # signature: UdfData(proj, datacube_list, feature_collection_list, structured_data_list, ml_model_list, metadata)
        
        # run the udf through the same routine as it would have been parsed in the backend
        if udf_data is not None:
            result=run_user_code(udf, udf_data)
            return result
        
        return None


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

"""
The main module for creating earth observation processes. It aims to easily build complex process chains, that can
be evaluated by an openEO backend.

.. data:: THIS

    Symbolic reference to the current data cube, to be used as argument in :py:meth:`DataCube.process()` calls

"""
import datetime
import logging
import pathlib
import re
import typing
import warnings
from builtins import staticmethod
from typing import List, Dict, Union, Tuple, Optional, Any

import numpy as np
import requests
import shapely.geometry
import shapely.geometry.base
from shapely.geometry import Polygon, MultiPolygon, mapping

import openeo
import openeo.processes
from openeo.api.process import Parameter
from openeo.internal.documentation import openeo_process
from openeo.internal.graph_building import PGNode, ReduceNode, _FromNodeMixin
from openeo.internal.processes.builder import get_parameter_names, convert_callable_to_pgnode
from openeo.internal.warnings import legacy_alias, UserDeprecationWarning, deprecated
from openeo.metadata import CollectionMetadata, Band, BandDimension, TemporalDimension, SpatialDimension
from openeo.processes import ProcessBuilder
from openeo.rest import BandMathException, OperatorException, OpenEoClientException
from openeo.rest._datacube import _ProcessGraphAbstraction, THIS
from openeo.rest.job import BatchJob, RESTJob
from openeo.rest.mlmodel import MlModel
from openeo.rest.service import Service
from openeo.rest.udp import RESTUserDefinedProcess
from openeo.rest.vectorcube import VectorCube
from openeo.util import get_temporal_extent, dict_no_none, rfc3339, guess_format

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection
    import xarray
    from openeo.udf import XarrayDataCube


log = logging.getLogger(__name__)


class UDF:
    """
    Helper class to load UDF code (e.g. from file) and embed them as "callback" or child process in a process graph.

    Usage example:

    .. code-block:: python

        udf = UDF.from_file("my-udf-code.py")
        cube = cube.apply(process=udf)


    .. versionchanged:: 0.13.0
        Added auto-detection of ``runtime``.
        Specifying the ``data`` argument is not necessary anymore, and actually deprecated.
        Added :py:meth:`from_file` to simplify loading UDF code from a file.
        See :ref:`old_udf_api` for more background about the changes.
    """

    __slots__ = ["code", "runtime", "version", "context", "_source"]

    def __init__(
            self, code: str, runtime: Optional[str] = None, data=None,
            version: Optional[str] = None, context: Optional[dict] = None, _source=None,
    ):
        """
        Construct a UDF object from given code string and other argument related to the ``run_udf`` process.

        :param code: UDF source code string (Python, R, ...)
        :param runtime: optional UDF runtime identifier, will be autodetected from source code if omitted.
        :param data: unused leftover from old API. Don't use this argument, it will be removed in a future release.
        :param version: optional UDF runtime version string
        :param context: optional additional UDF context data
        :param _source: (for internal use) source identifier
        """
        # TODO: automatically dedent code (when literal string) ?
        self.code = code
        self.runtime = runtime
        self.version = version
        self.context = context
        self._source = _source
        if data is not None:
            # TODO #181 remove `data` argument
            warnings.warn(
                f"The `data` argument of `{self.__class__.__name__}` is deprecated, unused and will be removed in a future release.",
                category=UserDeprecationWarning, stacklevel=2
            )

    @classmethod
    def from_file(
            cls, path: Union[str, pathlib.Path], runtime: Optional[str] = None, version: Optional[str] = None,
            context: Optional[dict] = None
    ) -> "UDF":
        """
        Load a UDF from a local file.

        .. seealso::
            :py:meth:`from_url` for loading from a URL.

        :param path: path to the local file with UDF source code
        :param runtime: optional UDF runtime identifier, will be auto-detected from source code if omitted.
        :param version: optional UDF runtime version string
        :param context: optional additional UDF context data
        """
        path = pathlib.Path(path)
        code = path.read_text(encoding="utf-8")
        return cls(code=code, runtime=runtime, version=version, context=context, _source=path)

    @classmethod
    def from_url(
            cls, url: str, runtime: Optional[str] = None, version: Optional[str] = None,
            context: Optional[dict] = None
    ) -> "UDF":
        """
        Load a UDF from a URL.

        .. seealso::
            :py:meth:`from_file` for loading from a local file.

        :param url: URL path to load the UDF source code from
        :param runtime: optional UDF runtime identifier, will be auto-detected from source code if omitted.
        :param version: optional UDF runtime version string
        :param context: optional additional UDF context data
        """
        resp = requests.get(url)
        resp.raise_for_status()
        code = resp.text
        return cls(code=code, runtime=runtime, version=version, context=context, _source=url)

    def _guess_runtime(self, connection: "openeo.Connection") -> str:
        """Guess UDF runtime from UDF source (path) or source code."""
        # First, guess UDF language
        language = None
        if isinstance(self._source, pathlib.Path):
            language = self._guess_runtime_from_suffix(self._source.suffix)
        elif isinstance(self._source, str):
            url_match = re.match(r"https?://.*?(?P<suffix>\.\w+)([&#].*)?$", self._source)
            if url_match:
                language = self._guess_runtime_from_suffix(url_match.group("suffix"))
        if not language:
            # Guess language from UDF code
            if re.search(r"^def [\w0-9_]+\(", self.code, flags=re.MULTILINE):
                language = "Python"
            # TODO: detection heuristics for R and other languages?
        if not language:
            raise OpenEoClientException("Failed to detect language of UDF code.")
        # Find runtime for language
        runtimes = {k.lower(): k for k in connection.list_udf_runtimes().keys()}
        if language.lower() in runtimes:
            return runtimes[language.lower()]
        else:
            raise OpenEoClientException(f"Failed to match UDF language {language!r} with a runtime ({runtimes})")

    def _guess_runtime_from_suffix(self, suffix: str) -> Union[str]:
        return {
            ".py": "Python",
            ".r": "R",
        }.get(suffix.lower())

    def get_run_udf_callback(self, connection: "openeo.Connection", data_parameter: str = "data") -> PGNode:
        """
        For internal use: construct `run_udf` node to be used as callback in `apply`, `reduce_dimension`, ...
        """
        arguments = dict_no_none(
            data={"from_parameter": data_parameter},
            udf=self.code,
            runtime=self.runtime or self._guess_runtime(connection=connection),
            version=self.version,
            context=self.context,
        )
        return PGNode(process_id="run_udf", arguments=arguments)


class DataCube(_ProcessGraphAbstraction):
    """
    Class representing a openEO (raster) data cube.

    The data cube is represented by its corresponding openeo "process graph"
    and this process graph can be "grown" to a desired workflow by calling the appropriate methods.
    """

    def __init__(self, graph: PGNode, connection: 'openeo.Connection', metadata: CollectionMetadata = None):
        super().__init__(pgnode=graph, connection=connection)
        self.metadata = CollectionMetadata.get_or_create(metadata)

    def process(
            self,
            process_id: str,
            arguments: dict = None,
            metadata: Optional[CollectionMetadata] = None,
            namespace: Optional[str] = None,
            **kwargs
    ) -> 'DataCube':
        """
        Generic helper to create a new DataCube by applying a process.

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :param metadata: optional: metadata to override original cube metadata (e.g. when reducing dimensions)
        :param namespace: optional: process namespace
        :return: new DataCube instance
        """
        pg = self._build_pgnode(process_id=process_id, arguments=arguments, namespace=namespace, **kwargs)
        return DataCube(graph=pg, connection=self._connection, metadata=metadata or self.metadata)

    graph_add_node = legacy_alias(process, "graph_add_node")

    def process_with_node(self, pg: PGNode, metadata: Optional[CollectionMetadata] = None) -> 'DataCube':
        """
        Generic helper to create a new DataCube by applying a process (given as process graph node)

        :param pg: process graph node (containing process id and arguments)
        :param metadata: optional: metadata to override original cube metadata (e.g. when reducing dimensions)
        :return: new DataCube instance
        """
        # TODO: deep copy `self.metadata` instead of using same instance?
        # TODO: cover more cases where metadata has to be altered
        # TODO: deprecate `process_with_node``: little added value over just calling DataCube() directly
        return DataCube(graph=pg, connection=self._connection, metadata=metadata or self.metadata)

    @classmethod
    @openeo_process
    def load_collection(
            cls,
            collection_id: str,
            connection: 'openeo.Connection' = None,
            spatial_extent: Optional[Dict[str, float]] = None,
            temporal_extent: Optional[List[Union[str, datetime.datetime, datetime.date, PGNode]]] = None,
            bands: Optional[List[str]] = None,
            fetch_metadata=True,
            properties: Optional[Dict[str, Union[str, PGNode, typing.Callable]]] = None,
            max_cloud_cover: Optional[float] = None,
    ) -> 'DataCube':
        """
        Create a new Raster Data cube.

        :param collection_id: image collection identifier
        :param connection: The connection to use to connect with the backend.
        :param spatial_extent: limit data to specified bounding box or polygons
        :param temporal_extent: limit data to specified temporal interval
        :param bands: only add the specified bands
        :param properties: limit data by metadata property predicates
        :param max_cloud_cover: shortcut to set maximum cloud cover ("eo:cloud_cover" collection property)
        :return: new DataCube containing the collection

        .. versionadded:: 0.13.0
            added the ``max_cloud_cover`` argument.

        """
        if temporal_extent:
            temporal_extent = cls._get_temporal_extent(extent=temporal_extent)
        arguments = {
            'id': collection_id,
            # TODO: spatial_extent could also be a "geojson" subtype object, so we might want to allow (and convert) shapely shapes as well here.
            'spatial_extent': spatial_extent,
            'temporal_extent': temporal_extent,
        }
        if isinstance(collection_id, Parameter):
            fetch_metadata = False
        metadata = connection.collection_metadata(collection_id) if fetch_metadata else None
        if bands:
            if isinstance(bands, str):
                bands = [bands]
            if metadata:
                bands = [b if isinstance(b, str) else metadata.band_dimension.band_name(b) for b in bands]
                metadata = metadata.filter_bands(bands)
            else:
                # Ensure minimal metadata with best effort band dimension guess (based on `bands` argument).
                band_dimension = BandDimension("bands", bands=[Band(b, None, None) for b in bands])
                metadata = CollectionMetadata({}, dimensions=[band_dimension])
            arguments['bands'] = bands
        if max_cloud_cover:
            properties = properties or {}
            properties["eo:cloud_cover"] = lambda v: v <= max_cloud_cover
        if properties:
            summaries = metadata and metadata.get("summaries") or {}
            undefined_properties = set(properties.keys()).difference(summaries.keys())
            if undefined_properties:
                warnings.warn(
                    f"{collection_id} property filtering with properties that are undefined "
                    f"in the collection metadata (summaries): {', '.join(undefined_properties)}.",
                    stacklevel=2,
                )
            arguments['properties'] = {
                prop: cls._get_callback(pred, parent_parameters=["value"])
                for prop, pred in properties.items()
            }
        pg = PGNode(
            process_id='load_collection',
            arguments=arguments
        )
        return cls(graph=pg, connection=connection, metadata=metadata)

    create_collection = legacy_alias(load_collection, name="create_collection")

    @classmethod
    def load_disk_collection(cls, connection: 'openeo.Connection', file_format: str, glob_pattern: str,
                             **options) -> 'DataCube':
        """
        Loads image data from disk as a DataCube.
        This is backed by a non-standard process ('load_disk_data'). This will eventually be replaced by standard options such as
        https://processes.openeo.org/#load_uploaded_files


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

        metadata = CollectionMetadata({}, dimensions=[
            SpatialDimension(name="x", extent=[]),
            SpatialDimension(name="y", extent=[]),
            TemporalDimension(name='t', extent=[]),
            BandDimension(name="bands", bands=[Band("unknown")]),
        ])
        return cls(graph=pg, connection=connection, metadata=metadata)

    @classmethod
    def _get_temporal_extent(
            cls, *args,
            start_date: Union[str, datetime.datetime, datetime.date, Parameter] = None,
            end_date: Union[str, datetime.datetime, datetime.date, Parameter] = None,
            extent: Union[list, tuple, Parameter] = None
    ) -> Union[List[Union[str, None, Parameter]], Parameter]:
        """Parameter aware temporal_extent normalizer"""
        if len(args) == 1 and isinstance(args[0], Parameter):
            assert start_date is None and end_date is None and extent is None
            return args[0]
        elif len(args) == 0 and isinstance(extent, Parameter):
            assert start_date is None and end_date is None
            return extent
        else:
            def convertor(d: Any) -> Any:
                # TODO: can this be generalized through _FromNodeMixin?
                if isinstance(d, Parameter) or isinstance(d, PGNode):
                    return d
                elif isinstance(d, ProcessBuilder):
                    return d.pgnode
                else:
                    return rfc3339.normalize(d)

            return list(get_temporal_extent(
                *args, start_date=start_date, end_date=end_date, extent=extent, convertor=convertor
            ))

    @openeo_process
    def filter_temporal(
            self, *args,
            start_date: Union[str, datetime.datetime, datetime.date] = None,
            end_date: Union[str, datetime.datetime, datetime.date] = None,
            extent: Union[list, tuple] = None
    ) -> 'DataCube':
        """
        Limit the DataCube to a certain date range, which can be specified in several ways:

        >>> im.filter_temporal("2019-07-01", "2019-08-01")
        >>> im.filter_temporal(["2019-07-01", "2019-08-01"])
        >>> im.filter_temporal(extent=["2019-07-01", "2019-08-01"])
        >>> im.filter_temporal(start_date="2019-07-01", end_date="2019-08-01"])

        :param start_date: start date of the filter (inclusive), as a string or date object
        :param end_date: end date of the filter (exclusive), as a string or date object
        :param extent: two element list/tuple start and end date of the filter
        :return: An ImageCollection filtered by date.

        https://open-eo.github.io/openeo-api/processreference/#filter_temporal
        """
        return self.process(
            process_id='filter_temporal',
            arguments={
                'data': THIS,
                'extent': self._get_temporal_extent(*args, start_date=start_date, end_date=end_date, extent=extent)
            }
        )

    @openeo_process
    def filter_bbox(
            self,
            *args,
            west=None, south=None, east=None, north=None,
            crs=None,
            base=None, height=None,
            bbox=None
    ) -> 'DataCube':
        """
        Limits the data cube to the specified bounding box.

        The bounding box can be specified in multiple ways.

            - With keyword arguments::

                >>> cube.filter_bbox(west=3, south=51, east=4, north=52, crs=4326)

            - With a (west, south, east, north) list or tuple
              (note that EPSG:4326 is the default CRS, so it's not nececarry to specify it explicitly)::

                >>> cube.filter_bbox([3, 51, 4, 52])
                >>> cube.filter_bbox(bbox=[3, 51, 4, 52])

            - With a bbox dictionary::

                >>> bbox = {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}
                >>> cube.filter_bbox(bbox)
                >>> cube.filter_bbox(bbox=bbox)
                >>> cube.filter_bbox(**bbox)

            - With a shapely geometry (of which the bounding box will be used)::

                >>> cube.filter_bbox(geometry)
                >>> cube.filter_bbox(bbox=geometry)

            - Passing a parameter::

                >>> bbox_param = Parameter(name="my_bbox", schema="object")
                >>> cube.filter_bbox(bbox_param)
                >>> cube.filter_bbox(bbox=bbox_param)

            - With a CRS other than EPSG 4326::

                >>> cube.filter_bbox(west=652000, east=672000, north=5161000, south=5181000, crs=32632)

            - Deprecated: positional arguments are also supported,
              but follow a non-standard order for legacy reasons::

                >>> west, east, north, south = 3, 4, 52, 51
                >>> cube.filter_bbox(west, east, north, south)

        """
        if args and any(k is not None for k in (west, south, east, north, bbox)):
            raise ValueError("Don't mix positional arguments with keyword arguments.")
        if bbox and any(k is not None for k in (west, south, east, north)):
            raise ValueError("Don't mix `bbox` with `west`/`south`/`east`/`north` keyword arguments.")

        if args:
            if 4 <= len(args) <= 5:
                # Handle old-style west-east-north-south order
                # TODO remove handling of this legacy order?
                warnings.warn("Deprecated argument order usage: `filter_bbox(west, east, north, south)`."
                              " Use keyword arguments or tuple/list argument instead.")
                west, east, north, south = args[:4]
                if len(args) > 4:
                    crs = args[4]
            elif len(args) == 1 and (isinstance(args[0], (list, tuple)) and len(args[0]) == 4
                                     or isinstance(args[0], (dict, shapely.geometry.base.BaseGeometry, Parameter))):
                bbox = args[0]
            else:
                raise ValueError(args)

        if isinstance(bbox, Parameter):
            extent = bbox
        else:
            if bbox:
                if isinstance(bbox, shapely.geometry.base.BaseGeometry):
                    west, south, east, north = bbox.bounds
                elif isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                    west, south, east, north = bbox[:4]
                elif isinstance(bbox, dict):
                    west, south, east, north = (bbox[k] for k in ["west", "south", "east", "north"])
                    if "crs" in bbox:
                        crs = bbox["crs"]
                else:
                    raise ValueError(bbox)

            extent = {'west': west, 'east': east, 'north': north, 'south': south}
            extent.update(dict_no_none(crs=crs, base=base, height=height))

        return self.process(
            process_id='filter_bbox',
            arguments={
                'data': THIS,
                'extent': extent
            }
        )

    @openeo_process
    def filter_spatial(
            self,
            geometries
    ) -> 'DataCube':
        """
        Limits the data cube over the spatial dimensions to the specified geometries.

            - For polygons, the filter retains a pixel in the data cube if the point at the pixel center intersects with
              at least one of the polygons (as defined in the Simple Features standard by the OGC).
            - For points, the process considers the closest pixel center.
            - For lines (line strings), the process considers all the pixels whose centers are closest to at least one
              point on the line.

        More specifically, pixels outside of the bounding box of the given geometry will not be available after filtering.
        All pixels inside the bounding box that are not retained will be set to null (no data).

        :param geometries: One or more geometries used for filtering, specified as GeoJSON in EPSG:4326.
        :return: A data cube restricted to the specified geometries. The dimensions and dimension properties (name,
            type, labels, reference system and resolution) remain unchanged, except that the spatial dimensions have less
            (or the same) dimension labels.
        """
        valid_geojson_types = [
            "Point", "MultiPoint", "LineString", "MultiLineString",
            "Polygon", "MultiPolygon", "GeometryCollection", "FeatureCollection"
        ]
        geometries = self._get_geometry_argument(geometries, valid_geojson_types=valid_geojson_types, crs=None)
        return self.process(
            process_id='filter_spatial',
            arguments={
                'data': THIS,
                'geometries': geometries
            }
        )

    @openeo_process
    def filter_bands(self, bands: Union[List[Union[str, int]], str]) -> 'DataCube':
        """
        Filter the data cube by the given bands

        :param bands: list of band names, common names or band indices. Single band name can also be given as string.
        :return: a DataCube instance
        """
        if isinstance(bands, str):
            bands = [bands]
        bands = [self.metadata.band_dimension.band_name(b) for b in bands]
        cube = self.process(
            process_id="filter_bands",
            arguments={"data": THIS, "bands": bands},
        )
        if cube.metadata:
            cube.metadata = cube.metadata.filter_bands(bands)
        return cube

    band_filter = legacy_alias(filter_bands, "band_filter")

    def band(self, band: Union[str, int]) -> "DataCube":
        """
        Filter out a single band

        :param band: band name, band common name or band index.
        :return: a DataCube instance
        """
        band_index = self.metadata.get_band_index(band)
        return self.reduce_bands(reducer=PGNode(
            process_id='array_element',
            arguments={
                'data': {'from_parameter': 'data'},
                'index': band_index
            },
        ))

    @openeo_process
    def resample_spatial(
            self, resolution: Union[float, Tuple[float, float]], projection: Union[int, str] = None,
            method: str = 'near', align: str = 'upper-left'
    ) -> 'DataCube':
        return self.process('resample_spatial', {
            'data': THIS,
            'resolution': resolution,
            'projection': projection,
            'method': method,
            'align': align
        })

    def resample_cube_spatial(self, target: "DataCube", method: str = "near") -> 'DataCube':
        """
        Resamples the spatial dimensions (x,y) from a source data cube to align with the corresponding
        dimensions of the given target data cube.
        Returns a new data cube with the resampled dimensions.

        To resample a data cube to a specific resolution or projection regardless of an existing target
        data cube, refer to :py:meth:`resample_spatial`.

        :param target: A data cube that describes the spatial target resolution.
        :param method: Resampling method to use.
        :return:
        """
        return self.process("resample_cube_spatial", {"data": self, "target": target, "method": method})

    @openeo_process
    def resample_cube_temporal(
            self, target: "DataCube", dimension: Optional[str] = None, valid_within: Optional[int] = None
    ) -> 'DataCube':
        """
        Resamples one or more given temporal dimensions from a source data cube to align with the corresponding
        dimensions of the given target data cube using the nearest neighbor method.
        Returns a new data cube with the resampled dimensions.

        By default, this process simply takes the nearest neighbor independent of the value (including values such as
        no-data / ``null``). Depending on the data cubes this may lead to values being assigned to two target timestamps.
        To only consider valid values in a specific range around the target timestamps, use the parameter ``valid_within``.

        The rare case of ties is resolved by choosing the earlier timestamps.

        :param target: A data cube that describes the temporal target resolution.
        :param dimension: The name of the temporal dimension to resample.
        :param valid_within:
        :return:

        .. versionadded:: 0.10.0
        """
        return self.process(
            "resample_cube_temporal",
            dict_no_none({"data": self, "target": target, "dimension": dimension, "valid_within": valid_within})
        )

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
            elif isinstance(other, (int, float)):
                # "`apply` math" mode
                return self._apply_operator(
                    operator=operator, other=other, reverse=reverse
                )
        raise OperatorException(
            f"Unsupported operator {operator!r} with `other` type {type(other)!r} (band math mode={band_math_mode})"
        )

    def _operator_unary(self, operator: str, **kwargs) -> 'DataCube':
        band_math_mode = self._in_bandmath_mode()
        if band_math_mode:
            return self._bandmath_operator_unary(operator, **kwargs)
        else:
            return self._apply_operator(operator=operator, extra_arguments=kwargs)

    def _apply_operator(
        self,
        operator: str,
        other: Optional[Union[int, float]] = None,
        reverse: Optional[bool] = None,
        extra_arguments: Optional[dict] = None,
    ) -> "DataCube":
        """
        Apply a unary or binary operator/process,
        by appending to existing `apply` node, or starting a new one.

        :param operator: process id of operator
        :param other: for binary operators: "other" argument
        :param reverse: for binary operators: "self" and "other" should be swapped (reflected operator mode)
        """
        if self.result_node().process_id == "apply":
            # Append to existing `apply` node
            orig_apply = self.result_node()
            data = orig_apply.arguments["data"]
            x = {"from_node": orig_apply.arguments["process"]["process_graph"]}
            context = orig_apply.arguments.get("context")
        else:
            # Start new `apply` node.
            data = self
            x = {"from_parameter": "x"}
            context = None
        # Build args for child callback.
        args = {"x": x, **(extra_arguments or {})}
        if other is not None:
            # Binary operator mode
            args["y"] = other
            if reverse:
                args["x"], args["y"] = args["y"], args["x"]
        child_pg = PGNode(process_id=operator, arguments=args)
        return self.process_with_node(
            PGNode(
                process_id="apply",
                arguments=dict_no_none(
                    data=data,
                    process={"process_graph": child_pg},
                    context=context,
                ),
            )
        )

    @openeo_process(mode="operator")
    def add(self, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        return self._operator_binary("add", other, reverse=reverse)

    @openeo_process(mode="operator")
    def subtract(self, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        return self._operator_binary("subtract", other, reverse=reverse)

    @openeo_process(mode="operator")
    def divide(self, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        return self._operator_binary("divide", other, reverse=reverse)

    @openeo_process(mode="operator")
    def multiply(self, other: Union['DataCube', int, float], reverse=False) -> 'DataCube':
        return self._operator_binary("multiply", other, reverse=reverse)

    @openeo_process
    def normalized_difference(self, other: 'DataCube') -> 'DataCube':
        # This DataCube method is only a convenience function when in band math mode
        assert self._in_bandmath_mode()
        assert other._in_bandmath_mode()
        return self._operator_binary("normalized_difference", other)

    @openeo_process(process_id="or", mode="operator")
    def logical_or(self, other: 'DataCube') -> 'DataCube':
        """
        Apply element-wise logical `or` operation

        :param other:
        :return: logical_or(this, other)
        """
        return self._operator_binary("or", other)

    @openeo_process(process_id="and", mode="operator")
    def logical_and(self, other: "DataCube") -> "DataCube":
        """
        Apply element-wise logical `and` operation

        :param other:
        :return: logical_and(this, other)
        """
        return self._operator_binary("and", other)

    @openeo_process(process_id="not", mode="operator")
    def __invert__(self) -> "DataCube":
        return self._operator_unary("not")

    @openeo_process(process_id="neq", mode="operator")
    def __ne__(self, other: Union["DataCube", int, float]) -> "DataCube":
        return self._operator_binary("neq", other)

    @openeo_process(process_id="eq", mode="operator")
    def __eq__(self, other: Union["DataCube", int, float]) -> "DataCube":
        """
        Pixelwise comparison of this data cube with another cube or constant.

        :param other: Another data cube, or a constant
        :return:
        """
        return self._operator_binary("eq", other)

    @openeo_process(process_id="gt", mode="operator")
    def __gt__(self, other: Union["DataCube", int, float]) -> "DataCube":
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.

        :param other:
        :return: this > other
        """
        return self._operator_binary("gt", other)

    @openeo_process(process_id="ge", mode="operator")
    def __ge__(self, other: Union["DataCube", int, float]) -> "DataCube":
        return self._operator_binary("gte", other)

    @openeo_process(process_id="lt", mode="operator")
    def __lt__(self, other: Union["DataCube", int, float]) -> "DataCube":
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.
        The number of bands in both data cubes has to be the same.

        :param other:
        :return: this < other
        """
        return self._operator_binary("lt", other)

    @openeo_process(process_id="le", mode="operator")
    def __le__(self, other: Union["DataCube", int, float]) -> "DataCube":
        return self._operator_binary("lte", other)

    @openeo_process(process_id="add", mode="operator")
    def __add__(self, other) -> "DataCube":
        return self.add(other)

    @openeo_process(process_id="add", mode="operator")
    def __radd__(self, other) -> "DataCube":
        return self.add(other, reverse=True)

    @openeo_process(process_id="subtract", mode="operator")
    def __sub__(self, other) -> "DataCube":
        return self.subtract(other)

    @openeo_process(process_id="subtract", mode="operator")
    def __rsub__(self, other) -> "DataCube":
        return self.subtract(other, reverse=True)

    @openeo_process(process_id="multiply", mode="operator")
    def __neg__(self) -> "DataCube":
        return self.multiply(-1)

    @openeo_process(process_id="multiply", mode="operator")
    def __mul__(self, other) -> "DataCube":
        return self.multiply(other)

    @openeo_process(process_id="multiply", mode="operator")
    def __rmul__(self, other) -> "DataCube":
        return self.multiply(other, reverse=True)

    @openeo_process(process_id="divide", mode="operator")
    def __truediv__(self, other) -> "DataCube":
        return self.divide(other)

    @openeo_process(process_id="divide", mode="operator")
    def __rtruediv__(self, other) -> "DataCube":
        return self.divide(other, reverse=True)

    @openeo_process(process_id="power", mode="operator")
    def __rpow__(self, other) -> "DataCube":
        return self._power(other, reverse=True)

    @openeo_process(process_id="power", mode="operator")
    def __pow__(self, other) -> "DataCube":
        return self._power(other, reverse=False)

    def _power(self, other, reverse=False):
        node = self._get_bandmath_node()
        x = node.reducer_process_graph()
        y = other
        if reverse:
            x, y = y, x
        return self.process_with_node(node.clone_with_new_reducer(
            PGNode(process_id="power", base=x, p=y)
        ))

    @openeo_process(process_id="power", mode="operator")
    def power(self, p: float):
        return self._power(other=p, reverse=False)

    @openeo_process(process_id="ln", mode="operator")
    def ln(self) -> "DataCube":
        return self._operator_unary("ln")

    @openeo_process(process_id="log", mode="operator")
    def logarithm(self, base: float) -> "DataCube":
        return self._operator_unary("log", base=base)

    @openeo_process(process_id="log", mode="operator")
    def log2(self) -> "DataCube":
        return self.logarithm(base=2)

    @openeo_process(process_id="log", mode="operator")
    def log10(self) -> "DataCube":
        return self.logarithm(base=10)

    @openeo_process(process_id="or", mode="operator")
    def __or__(self, other) -> "DataCube":
        return self.logical_or(other)

    @openeo_process(process_id="and", mode="operator")
    def __and__(self, other):
        return self.logical_and(other)

    def _bandmath_operator_binary_cubes(
            self, operator, other: "DataCube", left_arg_name="x", right_arg_name="y"
    ) -> "DataCube":
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
            },
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

    def _bandmath_operator_unary(self, operator: str, **kwargs) -> 'DataCube':
        node = self._get_bandmath_node()
        return self.process_with_node(node.clone_with_new_reducer(
            PGNode(operator, x={'from_node': node.reducer_process_graph()}, **kwargs)
        ))

    def _in_bandmath_mode(self) -> bool:
        """So-called "band math" mode: current result node is reduce_dimension along "bands" dimension."""
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

    def _get_geometry_argument(
            self,
            geometry: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, _FromNodeMixin],
            valid_geojson_types: List[str],
            crs: str = None,
    ) -> Union[dict, Parameter, PGNode]:
        """
        Convert input to a geometry as "geojson" subtype object.
        """
        if isinstance(geometry, (str, pathlib.Path)):
            # Assumption: `geometry` is path to polygon is a path to vector file at backend.
            # TODO #104: `read_vector` is non-standard process.
            # TODO: If path exists client side: load it client side?
            return PGNode(process_id="read_vector", arguments={"filename": str(geometry)})
        elif isinstance(geometry, Parameter):
            return geometry
        elif isinstance(geometry, _FromNodeMixin):
            return geometry.from_node()

        if isinstance(geometry, shapely.geometry.base.BaseGeometry):
            geometry = mapping(geometry)
        if not isinstance(geometry, dict):
            raise OpenEoClientException("Invalid geometry argument: {g!r}".format(g=geometry))

        if geometry.get("type") not in valid_geojson_types:
            raise OpenEoClientException("Invalid geometry type {t!r}, must be one of {s}".format(
                t=geometry.get("type"), s=valid_geojson_types
            ))
        if crs:
            # TODO: don't warn when the crs is Lon-Lat like EPSG:4326?
            warnings.warn("Geometry with non-Lon-Lat CRS {c!r} is only supported by specific back-ends.".format(c=crs))
            # TODO #204 alternative for non-standard CRS in GeoJSON object?
            geometry["crs"] = {"type": "name", "properties": {"name": crs}}
        return geometry

    @openeo_process
    def aggregate_spatial(
            self,
            geometries: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, "VectorCube"],
            reducer: Union[str, PGNode, typing.Callable],
            target_dimension: Optional[str] = None,
            crs: str = None,
            context: Optional[dict] = None,
            # TODO arguments: target dimension, context
    ) -> 'DataCube':
        """
        Aggregates statistics for one or more geometries (e.g. zonal statistics for polygons)
        over the spatial dimensions.

        :param geometries: a shapely geometry, a GeoJSON-style dictionary,
            a public GeoJSON URL, or a path (that is valid for the back-end) to a GeoJSON file.
        :param reducer: a callback function that creates a process graph, see :ref:`callbackfunctions`
        :param target_dimension: The new dimension name to be used for storing the results.
        :param crs: The spatial reference system of the provided polygon.
            By default longitude-latitude (EPSG:4326) is assumed.
        :param context: Additional data to be passed to the reducer process.

            .. note:: this ``crs`` argument is a non-standard/experimental feature, only supported by specific back-ends.
                See https://github.com/Open-EO/openeo-processes/issues/235 for details.
        """
        # TODO #279 aggregate_spatial should return a VectorCube, not a DataCube
        valid_geojson_types = [
            "Point", "MultiPoint", "LineString", "MultiLineString",
            "Polygon", "MultiPolygon", "GeometryCollection", "Feature", "FeatureCollection"
        ]
        geometries = self._get_geometry_argument(geometries, valid_geojson_types=valid_geojson_types, crs=crs)
        reducer = self._get_callback(reducer, parent_parameters=["data"])
        return self.process(
            process_id="aggregate_spatial", data=THIS, geometries=geometries, reducer=reducer,
            **dict_no_none(target_dimension=target_dimension, context=context)
        )

    @staticmethod
    def _get_callback(
            process: Union[str, PGNode, typing.Callable, UDF],
            parent_parameters: List[str],
            connection: Optional["openeo.Connection"] = None,
    ) -> dict:
        """
        Build a "callback" process: a user defined process that is used by another process (such
        as `apply`, `apply_dimension`, `reduce`, ....)

        :param process: process id string, PGNode or callable that uses the ProcessBuilder mechanism to build a process
        :param parent_parameters: list of parameter names defined for child process
        :return:
        """
        # TODO: autodetect the parameters defined by parent process?
        if isinstance(process, PGNode):
            # Assume this is already a valid callback process
            pg = process
        elif isinstance(process, str):
            # Assume given reducer is a simple predefined reduce process_id
            if process in openeo.processes.__dict__:
                process_params = get_parameter_names(openeo.processes.__dict__[process])
                # TODO: switch to "Callable" handling here
            else:
                # Best effort guess
                process_params = parent_parameters
            if parent_parameters == ["x", "y"] and (len(process_params) == 1 or process_params[:1] == ["data"]):
                # Special case: wrap all parent parameters in an array
                arguments = {process_params[0]: [{"from_parameter": p} for p in parent_parameters]}
            else:
                # Only pass parameters that correspond with an arg name
                common = set(process_params).intersection(parent_parameters)
                arguments = {p: {"from_parameter": p} for p in common}
            pg = PGNode(process_id=process, arguments=arguments)
        elif isinstance(process, typing.Callable):
            pg = convert_callable_to_pgnode(process, parent_parameters=parent_parameters)
        elif isinstance(process, UDF):
            pg = process.get_run_udf_callback(connection=connection, data_parameter=parent_parameters[0])
        else:
            raise ValueError(process)

        return PGNode.to_process_graph_argument(pg)

    @openeo_process
    def apply_dimension(
            self, code: str = None, runtime=None,
            process: Union[str, PGNode, typing.Callable, UDF] = None,
            version="latest",
            # TODO: dimension has no default (per spec)?
            dimension="t",
            target_dimension=None,
            context: Optional[dict] = None,
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

        .. note::
            .. versionchanged:: 0.13.0
                arguments ``code``, ``runtime`` and ``version`` are deprecated if favor of the standard approach
                of using an :py:class:`openeo.UDF <openeo.rest.datacube.UDF>` object in the ``process`` argument.
                See :ref:`old_udf_api` for more background about the changes.

        :param code: [**deprecated**] UDF code or process identifier (optional)
        :param runtime: [**deprecated**] UDF runtime to use (optional)
        :param process: a callback function that creates a process graph, see :ref:`callbackfunctions`
        :param version: [**deprecated**] Version of the UDF runtime to use
        :param dimension: The name of the source dimension to apply the process on. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target_dimension: The name of the target dimension or null (the default) to use the source dimension
            specified in the parameter dimension. By specifying a target dimension, the source dimension is removed.
            The target dimension with the specified name and the type other (see add_dimension) is created, if it doesn't exist yet.
        :param context: Additional data to be passed to the process.

        :return: A datacube with the UDF applied to the given dimension.
        :raises: DimensionNotAvailable
        """
        # TODO #137 #181 #312 remove support for code/runtime/version
        if runtime or (isinstance(code, str) and "\n" in code):
            warnings.warn(
                "Specifying UDF code through `code`, `runtime` and `version` arguments is deprecated. "
                "Instead create an `openeo.UDF` object and pass that to the `process` argument.",
                category=UserDeprecationWarning, stacklevel=2
            )
            process = UDF(code=code, runtime=runtime, version=version, context=context)
        else:
            process = process or code
        process = self._get_callback(
            process=process, parent_parameters=["data", "context"], connection=self.connection
        )
        arguments = {
            "data": THIS,
            "process": process,
            "dimension": self.metadata.assert_valid_dimension(dimension),
        }
        if target_dimension is not None:
            arguments["target_dimension"] = target_dimension
        if context is not None:
            arguments["context"] = context
        result_cube = self.process(process_id="apply_dimension", arguments=arguments)

        return result_cube

    @openeo_process
    def reduce_dimension(
            self,
            dimension: str,
            reducer: Union[str, PGNode, typing.Callable, UDF],
            context: Optional[dict] = None,
            process_id="reduce_dimension", band_math_mode: bool = False
    ) -> "DataCube":
        """
        Add a reduce process with given reducer callback along given dimension

        :param dimension: the label of the dimension to reduce
        :param reducer: "child callback" function, see :ref:`callbackfunctions`
        :param context: Additional data to be passed to the process.
        """
        # TODO: check if dimension is valid according to metadata? #116
        # TODO: #125 use/test case for `reduce_dimension_binary`?
        reducer = self._get_callback(
            process=reducer, parent_parameters=["data", "context"], connection=self.connection
        )

        return self.process_with_node(ReduceNode(
            process_id=process_id,
            data=self,
            reducer=reducer,
            dimension=self.metadata.assert_valid_dimension(dimension),
            context=context,
            # TODO #123 is it (still) necessary to make "band" math a special case?
            band_math_mode=band_math_mode
        ), metadata=self.metadata.reduce_dimension(dimension_name=dimension))

    # @openeo_process
    def chunk_polygon(
            self,
            chunks: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, "VectorCube"],
            process: Union[str, PGNode, typing.Callable],
            mask_value: float = None,
            context: Optional[dict] = None,
    ) -> 'DataCube':
        """
        Apply a process to spatial chunks of a data cube.

        .. warning:: experimental process: not generally supported, API subject to change.

        :param chunks: Polygons, provided as a shapely geometry, a GeoJSON-style dictionary,
            a public GeoJSON URL, or a path (that is valid for the back-end) to a GeoJSON file.
        :param process: "child callback" function, see :ref:`callbackfunctions`
        :param mask_value: The value used for cells outside the polygon.
            This provides a distinction between NoData cells within the polygon (due to e.g. clouds)
            and masked cells outside it. If no value is provided, NoData cells are used outside the polygon.
        :param context: Additional data to be passed to the process.
        """
        process = self._get_callback(
            process, parent_parameters=["data"], connection=self.connection
        )
        valid_geojson_types = [
            "Polygon",
            "MultiPolygon",
            "GeometryCollection",
            "Feature",
            "FeatureCollection",
        ]
        chunks = self._get_geometry_argument(
            chunks, valid_geojson_types=valid_geojson_types
        )
        mask_value = float(mask_value) if mask_value is not None else None
        return self.process(
            process_id="chunk_polygon",
            data=THIS,
            chunks=chunks,
            process=process,
            arguments=dict_no_none(
                mask_value=mask_value,
                context=context,
            ),
        )

    def reduce_bands(self, reducer: Union[str, PGNode, typing.Callable, UDF]) -> 'DataCube':
        """
        Shortcut for :py:meth:`reduce_dimension` along the band dimension

        :param reducer: "child callback" function, see :ref:`callbackfunctions`
        """
        return self.reduce_dimension(dimension=self.metadata.band_dimension.name, reducer=reducer, band_math_mode=True)

    def reduce_temporal(self, reducer: Union[str, PGNode, typing.Callable, UDF]) -> 'DataCube':
        """
        Shortcut for :py:meth:`reduce_dimension` along the temporal dimension

        :param reducer: "child callback" function, see :ref:`callbackfunctions`
        """
        return self.reduce_dimension(dimension=self.metadata.temporal_dimension.name, reducer=reducer)

    @deprecated("Use :py:meth:`reduce_bands` with :py:class:`UDF` as reducer.", version="0.13.0")
    def reduce_bands_udf(self, code: str, runtime: Optional[str] = None, version: Optional[str] = None) -> 'DataCube':
        """
        Use `reduce_dimension` process with given UDF along band/spectral dimension.
        """
        # TODO #181 #312 drop this deprecated pattern
        return self.reduce_bands(reducer=UDF(code=code, runtime=runtime, version=version))

    @openeo_process
    def add_dimension(self, name: str, label: str, type: Optional[str] = None):
        """
        Adds a new named dimension to the data cube.
        Afterwards, the dimension can be referenced with the specified name. If a dimension with the specified name exists,
        the process fails with a DimensionExists error. The dimension label of the dimension is set to the specified label.

        This call does not modify the datacube in place, but returns a new datacube with the additional dimension.

        :param name: The name of the dimension to add
        :param label: The dimension label.
        :param type: Dimension type, allowed values: 'spatial', 'temporal', 'bands', 'other', default value is 'other'
        :return: The data cube with a newly added dimension. The new dimension has exactly one dimension label. All other dimensions remain unchanged.
        """
        return self.process(
            process_id="add_dimension",
            arguments=dict_no_none({"data": self, "name": name, "label": label, "type": type}),
            metadata=self.metadata.add_dimension(name=name, label=label, type=type)
        )

    @openeo_process
    def drop_dimension(self, name: str):
        """
        Drops a dimension from the data cube.
        Dropping a dimension only works on dimensions with a single dimension label left, otherwise the process fails
        with a DimensionLabelCountMismatch exception. Dimension values can be reduced to a single value with a filter
        such as filter_bands or the reduce_dimension process. If a dimension with the specified name does not exist,
        the process fails with a DimensionNotAvailable exception.

        :param name: The name of the dimension to drop
        :return: The data cube with the given dimension dropped.
        """
        return self.process(
            process_id="drop_dimension",
            arguments={"data": self, "name": name},
            metadata=self.metadata.drop_dimension(name=name),
        )

    @deprecated("Use :py:meth:`reduce_temporal` with :py:class:`UDF` as reducer", version="0.13.0")
    def reduce_temporal_udf(self, code: str, runtime="Python", version="latest"):
        """
        Apply reduce (`reduce_dimension`) process with given UDF along temporal dimension.

        :param code: The UDF code, compatible with the given runtime and version
        :param runtime: The UDF runtime
        :param version: The UDF runtime version
        """
        # TODO #181 #312 drop this deprecated pattern
        return self.reduce_temporal(reducer=UDF(code=code, runtime=runtime, version=version))

    reduce_tiles_over_time = legacy_alias(reduce_temporal_udf, name="reduce_tiles_over_time")

    @openeo_process
    def apply_neighborhood(
            self,
            process: Union[str, PGNode, typing.Callable, UDF],
            size: List[Dict],
            overlap: List[dict] = None,
            context: Optional[dict] = None,
    ) -> "DataCube":
        """
        Applies a focal process to a data cube.

        A focal process is a process that works on a 'neighbourhood' of pixels. The neighbourhood can extend into multiple dimensions, this extent is specified by the `size` argument. It is not only (part of) the size of the input window, but also the size of the output for a given position of the sliding window. The sliding window moves with multiples of `size`.

        An overlap can be specified so that neighbourhoods can have overlapping boundaries. This allows for continuity of the output. The values included in the data cube as overlap can't be modified by the given `process`.

        The neighbourhood size should be kept small enough, to avoid running beyond computational resources, but a too small size will result in a larger number of process invocations, which may slow down processing. Window sizes for spatial dimensions typically are in the range of 64 to 512 pixels, while overlaps of 8 to 32 pixels are common.

        The process must not add new dimensions, or remove entire dimensions, but the result can have different dimension labels.

        For the special case of 2D convolution, it is recommended to use ``apply_kernel()``.

        :param size:
        :param overlap:
        :param process: a callback function that creates a process graph, see :ref:`callbackfunctions`
        :param context: Additional data to be passed to the process.

        :return:
        """
        return self.process(
            process_id="apply_neighborhood",
            arguments=dict_no_none(
                data=THIS,
                process=self._get_callback(process=process, parent_parameters=["data"], connection=self.connection),
                size=size,
                overlap=overlap,
                context=context,
            )
        )

    @openeo_process
    def apply(
            self,
            process: Union[str, PGNode, typing.Callable, UDF] = None,
            context: Optional[dict] = None,
    ) -> "DataCube":
        """
        Applies a unary process (a local operation) to each value of the specified or all dimensions in the data cube.

        :param process: the name of a process, or a callback function that creates a process graph, see :ref:`callbackfunctions`
        :param dimensions: The names of the dimensions to apply the process on. Defaults to an empty array so that all dimensions are used.
        :param context: Additional data to be passed to the process.

        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        return self.process(
            process_id="apply",
            arguments=dict_no_none({
                "data": THIS,
                "process": self._get_callback(process, parent_parameters=["x"], connection=self.connection),
                "context": context,
            })
        )

    reduce_temporal_simple = legacy_alias(reduce_temporal, "reduce_temporal_simple")

    @openeo_process(process_id="min", mode="reduce_dimension")
    def min_time(self) -> 'DataCube':
        """
        Finds the minimum value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("min")

    @openeo_process(process_id="max", mode="reduce_dimension")
    def max_time(self) -> 'DataCube':
        """
        Finds the maximum value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("max")

    @openeo_process(process_id="mean", mode="reduce_dimension")
    def mean_time(self) -> "DataCube":
        """
        Finds the mean value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("mean")

    @openeo_process(process_id="median", mode="reduce_dimension")
    def median_time(self) -> "DataCube":
        """
        Finds the median value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("median")

    @openeo_process(process_id="count", mode="reduce_dimension")
    def count_time(self) -> "DataCube":
        """
        Counts the number of images with a valid mask in a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("count")

    @openeo_process
    def aggregate_temporal(
            self,
            intervals: List[list],
            reducer: Union[str, PGNode, typing.Callable],
            labels: Optional[List[str]] = None,
            dimension: Optional[str] = None,
            context: Optional[dict] = None,
    ) -> "DataCube":
        """
        Computes a temporal aggregation based on an array of date and/or time intervals.

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
                data=THIS,
                intervals=intervals,
                labels=labels,
                dimension=dimension,
                reducer=self._get_callback(reducer, parent_parameters=["data"]),
                context=context,
            ),
        )

    @openeo_process
    def aggregate_temporal_period(
            self,
            period: str,
            reducer: Union[str, PGNode, typing.Callable],
            dimension: Optional[str] = None,
            context: Optional[Dict] = None,
    ) -> "DataCube":
        """
        Computes a temporal aggregation based on calendar hierarchies such as years, months or seasons. For other calendar hierarchies aggregate_temporal can be used.

        For each interval, all data along the dimension will be passed through the reducer.

        If the dimension is not set or is set to null, the data cube is expected to only have one temporal dimension.

        The period argument specifies the time intervals to aggregate. The following pre-defined values are available:

        - hour: Hour of the day
        - day: Day of the year
        - week: Week of the year
        - dekad: Ten day periods, counted per year with three periods per month (day 1 - 10, 11 - 20 and 21 - end of month). The third dekad of the month can range from 8 to 11 days. For example, the fourth dekad is Feb, 1 - Feb, 10 each year.
        - month: Month of the year
        - season: Three month periods of the calendar seasons (December - February, March - May, June - August, September - November).
        - tropical-season: Six month periods of the tropical seasons (November - April, May - October).
        - year: Proleptic years
        - decade: Ten year periods (0-to-9 decade), from a year ending in a 0 to the next year ending in a 9.
        - decade-ad: Ten year periods (1-to-0 decade) better aligned with the Anno Domini (AD) calendar era, from a year ending in a 1 to the next year ending in a 0.


        :param period: The period of the time intervals to aggregate.
        :param reducer: A reducer to be applied on all values along the specified dimension. The reducer must be a callable process (or a set processes) that accepts an array and computes a single return value of the same type as the input values, for example median.
        :param dimension: The temporal dimension for aggregation. All data along the dimension will be passed through the specified reducer. If the dimension is not set, the data cube is expected to only have one temporal dimension.
        :param context: Additional data to be passed to the reducer.

        :return: A data cube with the same dimensions. The dimension properties (name, type, labels, reference system and resolution) remain unchanged.
        """
        return self.process(
            process_id="aggregate_temporal_period",
            arguments=dict_no_none(
                data=THIS,
                period=period,
                dimension=dimension,
                reducer=self._get_callback(reducer, parent_parameters=["data"]),
                context=context,
            ),
        )

    @openeo_process
    def ndvi(self, nir: str = None, red: str = None, target_band: str = None) -> 'DataCube':
        """
        Normalized Difference Vegetation Index (NDVI)

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
            process_id="ndvi",
            arguments=dict_no_none(
                data=THIS, nir=nir, red=red, target_band=target_band
            ),
            metadata=metadata,
        )

    @openeo_process
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
            process_id="rename_dimension",
            arguments=dict_no_none(
                data=THIS,
                source=self.metadata.assert_valid_dimension(source),
                target=target,
            ),
            metadata=self.metadata.rename_dimension(source, target),
        )

    @openeo_process
    def rename_labels(self, dimension: str, target: list, source: list = None) -> 'DataCube':
        """
        Renames the labels of the specified dimension in the data cube from source to target.

        :param dimension: Dimension name
        :param target: The new names for the labels.
        :param source: The names of the labels as they are currently in the data cube.

        :return: An DataCube instance
        """
        return self.process(
            process_id="rename_labels",
            arguments=dict_no_none(
                data=THIS,
                dimension=self.metadata.assert_valid_dimension(dimension),
                target=target,
                source=source,
            ),
            metadata=self.metadata.rename_labels(dimension, target, source),
        )

    @openeo_process(mode="apply")
    def linear_scale_range(self, input_min, input_max, output_min, output_max) -> 'DataCube':
        """
        Performs a linear transformation between the input and output range.

        The given number in x is clipped to the bounds specified in inputMin and inputMax so that the underlying formula

         ((x - inputMin) / (inputMax - inputMin)) * (outputMax - outputMin) + outputMin

         never returns any value lower than outputMin or greater than outputMax.

        Potential use case include scaling values to the 8-bit range (0 - 255) often used for numeric representation of
        values in one of the channels of the RGB colour model or calculating percentages (0 - 100).

        The no-data value null is passed through and therefore gets propagated.

        :param input_min: Minimum input value
        :param input_max: Maximum input value
        :param output_min: Minimum value of the desired output range.
        :param output_max: Maximum value of the desired output range.
        :return: a DataCube instance
        """

        return self.apply(lambda x: x.linear_scale_range(input_min, input_max, output_min, output_max))

    @openeo_process
    def mask(self, mask: "DataCube" = None, replacement=None) -> "DataCube":
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
            arguments=dict_no_none(data=self, mask=mask, replacement=replacement),
        )

    @openeo_process
    def mask_polygon(
            self,
            mask: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, "VectorCube"],
            srs: str = None,
            replacement=None, inside: bool = None
    ) -> 'DataCube':
        """
        Applies a polygon mask to a raster data cube. To apply a raster mask use `mask`.

        All pixels for which the point at the pixel center does not intersect with any
        polygon (as defined in the Simple Features standard by the OGC) are replaced.
        This behaviour can be inverted by setting the parameter `inside` to true.

        The pixel values are replaced with the value specified for `replacement`,
        which defaults to `no data`.

        :param mask: The geometry to mask with: a shapely geometry, a GeoJSON-style dictionary,
            a public GeoJSON URL, or a path (that is valid for the back-end) to a GeoJSON file.
        :param srs: The spatial reference system of the provided polygon.
            By default longitude-latitude (EPSG:4326) is assumed.

            .. note:: this ``srs`` argument is a non-standard/experimental feature, only supported by specific back-ends.
                See https://github.com/Open-EO/openeo-processes/issues/235 for details.
        :param replacement: the value to replace the masked pixels with
        """
        valid_geojson_types = ["Polygon", "MultiPolygon", "GeometryCollection", "Feature", "FeatureCollection"]
        mask = self._get_geometry_argument(mask, valid_geojson_types=valid_geojson_types, crs=srs)
        return self.process(
            process_id="mask_polygon",
            arguments=dict_no_none(
                data=THIS,
                mask=mask,
                replacement=replacement,
                inside=inside
            )
        )

    @openeo_process
    def merge_cubes(
            self,
            other: 'DataCube',
            overlap_resolver: Union[str, PGNode, typing.Callable] = None,
            context: Optional[dict] = None,
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

        :param other: The data cube to merge with.
        :param overlap_resolver: A reduction operator that resolves the conflict if the data overlaps. The reducer must return a value of the same data type as the input values are. The reduction operator may be a single process such as multiply or consist of multiple sub-processes. null (the default) can be specified if no overlap resolver is required.
        :param context: Additional data to be passed to the process.

        :return: The merged data cube.
        """
        arguments = {"cube1": self, "cube2": other}
        if overlap_resolver:
            arguments["overlap_resolver"] = self._get_callback(overlap_resolver, parent_parameters=["x", "y"])
        # Minimal client side metadata merging
        merged_metadata = self.metadata
        if self.metadata.has_band_dimension() and isinstance(other, DataCube) and other.metadata.has_band_dimension():
            for b in other.metadata.band_dimension.bands:
                if b not in merged_metadata.bands:
                    merged_metadata = merged_metadata.append_band(b)
        # TODO: warn about missing overlap_resolver if we can detect that one is required?
        if context:
            arguments["context"] = context
        return self.process(process_id="merge_cubes", arguments=arguments, metadata=merged_metadata)

    merge = legacy_alias(merge_cubes, name="merge")

    @openeo_process
    def apply_kernel(
            self, kernel: Union[np.ndarray, List[List[float]]], factor=1.0, border=0,
            replace_invalid=0
    ) -> "DataCube":
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
        :param replace_invalid: This parameter specifies the value to replace non-numerical or infinite numerical values with. By default, those values are replaced with zeroes.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        return self.process('apply_kernel', {
            'data': THIS,
            'kernel': kernel.tolist() if isinstance(kernel, np.ndarray) else kernel,
            'factor': factor,
            'border': border,
            'replace_invalid': replace_invalid
        })

    @openeo_process
    def resolution_merge(
            self, high_resolution_bands: List[str], low_resolution_bands: List[str], method: str = None
    ) -> "DataCube":
        """
        Resolution merging algorithms try to improve the spatial resolution of lower resolution bands
        (e.g. Sentinel-2 20M) based on higher resolution bands. (e.g. Sentinel-2 10M).

        External references:

        `Pansharpening explained <https://bok.eo4geo.eu/IP2-1-3>`_

        `Example publication: 'Improving the Spatial Resolution of Land Surface Phenology by Fusing Medium- and
        Coarse-Resolution Inputs' <https://doi.org/10.1109/TGRS.2016.2537929>`_

        .. warning:: experimental process: not generally supported, API subject to change.

        :param high_resolution_bands: A list of band names to use as 'high-resolution' band. Either the unique band name (metadata field `name` in bands) or one of the common band names (metadata field `common_name` in bands). If unique band name and common name conflict, the unique band name has higher priority. The order of the specified array defines the order of the bands in the data cube. If multiple bands match a common name, all matched bands are included in the original order. These bands will remain unmodified.
        :param low_resolution_bands: A list of band names for which the spatial resolution should be increased. Either the unique band name (metadata field `name` in bands) or one of the common band names (metadata field `common_name` in bands). If unique band name and common name conflict, the unique band name has higher priority. The order of the specified array defines the order of the bands in the data cube. If multiple bands match a common name, all matched bands are included in the original order. These bands will be modified by the process.
        :param method: The method to use. The supported algorithms can vary between back-ends. Set to `null` (the default) to allow the back-end to choose, which will improve portability, but reduce reproducibility..
        :return: A datacube with the same bands and metadata as the input, but algorithmically increased spatial resolution for the selected bands.
        """
        return self.process('resolution_merge', {
            'data': THIS,
            'high_resolution_bands': high_resolution_bands,
            'low_resolution_bands': low_resolution_bands,
            'method': method,

        })

    def raster_to_vector(self) -> VectorCube:
        """
        Converts this raster data cube into a :py:class:`~openeo.rest.vectorcube.VectorCube`.
        The bounding polygon of homogenous areas of pixels is constructed.

        .. warning:: experimental process: not generally supported, API subject to change.

        :return: a :py:class:`~openeo.rest.vectorcube.VectorCube`
        """
        pg_node = PGNode(process_id="raster_to_vector", arguments={"data": self})
        return VectorCube(pg_node, connection=self._connection, metadata=self.metadata)

    ####VIEW methods #######

    @deprecated("Use :py:meth:`aggregate_spatial` with reducer ``'mean'``.", version="0.10.0")
    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a mean time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """
        return self.aggregate_spatial(geometries=polygon, reducer="mean")

    @deprecated("Use :py:meth:`aggregate_spatial` with reducer ``'histogram'``.", version="0.10.0")
    def polygonal_histogram_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a histogram time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """
        return self.aggregate_spatial(geometries=polygon, reducer="histogram")

    @deprecated("Use :py:meth:`aggregate_spatial` with reducer ``'median'``.", version="0.10.0")
    def polygonal_median_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a median time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """
        return self.aggregate_spatial(geometries=polygon, reducer="median")

    @deprecated("Use :py:meth:`aggregate_spatial` with reducer ``'sd'``.", version="0.10.0")
    def polygonal_standarddeviation_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'DataCube':
        """
        Extract a time series of standard deviations for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: DataCube
        """
        return self.aggregate_spatial(geometries=polygon, reducer="sd")

    @openeo_process
    def ard_surface_reflectance(
            self, atmospheric_correction_method: str, cloud_detection_method: str, elevation_model: str = None,
            atmospheric_correction_options: dict = None, cloud_detection_options: dict = None,
    ) -> 'DataCube':
        """
        Computes CARD4L compliant surface reflectance values from optical input.

        :param atmospheric_correction_method: The atmospheric correction method to use.
        :param cloud_detection_method: The cloud detection method to use.
        :param elevation_model: The digital elevation model to use, leave empty to allow the back-end to make a suitable choice.
        :param atmospheric_correction_options: Proprietary options for the atmospheric correction method.
        :param cloud_detection_options: Proprietary options for the cloud detection method.
        :return: Data cube containing bottom of atmosphere reflectances with atmospheric disturbances like clouds and cloud shadows removed. The data returned is CARD4L compliant and contains metadata.
        """
        return self.process('ard_surface_reflectance', {
            'data': THIS,
            'atmospheric_correction_method': atmospheric_correction_method,
            'cloud_detection_method': cloud_detection_method,
            'elevation_model': elevation_model,
            'atmospheric_correction_options': atmospheric_correction_options or {},
            'cloud_detection_options': cloud_detection_options or {},
        })

    @openeo_process
    def atmospheric_correction(
            self,
            method: str = None,
            elevation_model: str = None,
            options: dict = None
    ) -> 'DataCube':
        """
        Applies an atmospheric correction that converts top of atmosphere reflectance values into bottom of atmosphere/top of canopy reflectance values.

        Note that multiple atmospheric methods exist, but may not be supported by all backends. The method parameter gives
        you the option of requiring a specific method, but this may result in an error if the backend does not support it.

        :param method: The atmospheric correction method to use. To get reproducible results, you have to set a specific method. Set to `null` to allow the back-end to choose, which will improve portability, but reduce reproducibility as you *may* get different results if you run the processes multiple times.
        :param elevation_model: The digital elevation model to use, leave empty to allow the back-end to make a suitable choice.
        :param options: Proprietary options for the atmospheric correction method.
        :return: datacube with bottom of atmosphere reflectances
        """
        return self.process('atmospheric_correction', {
            'data': THIS,
            'method': method,
            'elevation_model': elevation_model,
            'options': options or {},
        })

    @openeo_process
    def save_result(self, format: str = "GTiff", options: dict = None) -> 'DataCube':
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

    def download(
            self, outputfile: Union[str, pathlib.Path, None] = None, format: Optional[str] = None,
            options: Optional[dict] = None
    ):
        """
        Download image collection, e.g. as GeoTIFF.
        If outputfile is provided, the result is stored on disk locally, otherwise, a bytes object is returned.
        The bytes object can be passed on to a suitable decoder for decoding.

        :param outputfile: Optional, an output file if the result needs to be stored on disk.
        :param format: Optional, an output format supported by the backend.
        :param options: Optional, file format options
        :return: None if the result is stored to disk, or a bytes object returned by the backend.
        """
        if self.result_node().process_id == "save_result":
            # There is already a `save_result` node: check if it is consistent with given format/options
            args = self.result_node().arguments
            if format is not None and format.lower() != args["format"].lower():
                raise ValueError(
                    f"Existing `save_result` node with different format {args['format']!r} != {format!r}"
                )
            if options is not None and options != args["options"]:
                raise ValueError(
                    f"Existing `save_result` node with different options {args['options']!r} != {options!r}"
                )
            cube = self
        else:
            # No `save_result` node yet: automatically add it.
            if not format:
                format = guess_format(outputfile) if outputfile else "GTiff"
            cube = self.save_result(format=format, options=options)

        return self._connection.download(cube.flat_graph(), outputfile)

    def validate(self) -> List[dict]:
        """
        Validate a process graph without executing it.

        :return: list of errors (dictionaries with "code" and "message" fields)
        """
        return self._connection.validate_process_graph(self.flat_graph())

    def tiled_viewing_service(self, type: str, **kwargs) -> Service:
        return self._connection.create_service(self.flat_graph(), type=type, **kwargs)

    def execute_batch(
            self,
            outputfile: Union[str, pathlib.Path] = None, out_format: str = None,
            print=print, max_poll_interval=60, connection_retry_interval=30,
            job_options=None, **format_options) -> BatchJob:
        """
        Evaluate the process graph by creating a batch job, and retrieving the results when it is finished.
        This method is mostly recommended if the batch job is expected to run in a reasonable amount of time.

        For very long-running jobs, you probably do not want to keep the client running.

        :param job_options:
        :param outputfile: The path of a file to which a result can be written
        :param out_format: (optional) Format of the job result.
        :param format_options: String Parameters for the job result format

        """
        if "format" in format_options and not out_format:
            out_format = format_options["format"]  # align with 'download' call arg name
        if not out_format:
            out_format = guess_format(outputfile) if outputfile else "GTiff"
        job = self.create_job(out_format, job_options=job_options, **format_options)
        return job.run_synchronous(
            outputfile=outputfile,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )

    def create_job(
            self, out_format=None, title: str = None, description: str = None, plan: str = None, budget=None,
            job_options=None, **format_options
    ) -> BatchJob:
        """
        Sends the datacube's process graph as a batch job to the back-end
        and return a :py:class:`~openeo.rest.job.BatchJob` instance.

        Note that the batch job will just be created at the back-end,
        it still needs to be started and tracked explicitly.
        Use :py:meth:`execute_batch` instead to have the openEO Python client take care of that job management.

        :param out_format: String Format of the job result.
        :param job_options: A dictionary containing (custom) job options
        :param format_options: String Parameters for the job result format
        :return: status: Job resulting job.
        """
        # TODO: add option to also automatically start the job?
        img = self
        if out_format:
            # add `save_result` node
            img = img.save_result(format=out_format, options=format_options)
        return self._connection.create_job(
            process_graph=img.flat_graph(),
            title=title, description=description, plan=plan, budget=budget, additional=job_options
        )

    send_job = legacy_alias(create_job, name="send_job")

    def save_user_defined_process(
            self,
            user_defined_process_id: str,
            public: bool = False,
            summary: Optional[str] = None,
            description: Optional[str] = None,
            returns: Optional[dict] = None,
            categories: Optional[List[str]] = None,
            examples: Optional[List[dict]] = None,
            links: Optional[List[dict]] = None,
    ) -> RESTUserDefinedProcess:
        """
        Saves this process graph in the backend as a user-defined process for the authenticated user.

        :param user_defined_process_id: unique identifier for the process
        :param public: visible to other users?
        :param summary: A short summary of what the process does.
        :param description: Detailed description to explain the entity. CommonMark 0.29 syntax MAY be used for rich text representation.
        :param returns: Description and schema of the return value.
        :param categories: A list of categories.
        :param examples: A list of examples.
        :param links: A list of links.
        :return: a RESTUserDefinedProcess instance
        """
        return self._connection.save_user_defined_process(
            user_defined_process_id=user_defined_process_id,
            process_graph=self.flat_graph(), public=public, summary=summary, description=description,
            returns=returns, categories=categories, examples=examples, links=links,
        )

    def execute(self) -> Dict:
        """Executes the process graph of the imagery. """
        return self._connection.execute(self.flat_graph())

    @staticmethod
    @deprecated(reason="Use :py:func:`openeo.udf.run_code.execute_local_udf` instead", version="0.7.0")
    def execute_local_udf(udf: str, datacube: Union[str, 'xarray.DataArray', 'XarrayDataCube'] = None, fmt='netcdf'):
        import openeo.udf.run_code
        return openeo.udf.run_code.execute_local_udf(udf=udf, datacube=datacube, fmt=fmt)

    @openeo_process
    def ard_normalized_radar_backscatter(
            self, elevation_model: str = None, contributing_area=False,
            ellipsoid_incidence_angle: bool = False, noise_removal: bool = True
    ) -> "DataCube":
        """
        Computes CARD4L compliant backscatter (gamma0) from SAR input.
        This method is a variant of :py:meth:`~openeo.rest.datacube.DataCube.sar_backscatter`,
        with restricted parameters to generate backscatter according to CARD4L specifications.

        Note that backscatter computation may require instrument specific metadata that is tightly coupled to the original SAR products.
        As a result, this process may only work in combination with loading data from specific collections, not with general data cubes.

        :param elevation_model: The digital elevation model to use. Set to None (the default) to allow the back-end to choose, which will improve portability, but reduce reproducibility.
        :param contributing_area: If set to `true`, a DEM-based local contributing area band named `contributing_area`
            is added. The values are given in square meters.
        :param ellipsoid_incidence_angle: If set to `True`, an ellipsoidal incidence angle band named `ellipsoid_incidence_angle` is added. The values are given in degrees.
        :param noise_removal: If set to `false`, no noise removal is applied. Defaults to `True`, which removes noise.

        :return: Backscatter values expressed as gamma0. The data returned is CARD4L compliant and contains metadata. By default, the backscatter values are given in linear scale.
        """
        return self.process(process_id="ard_normalized_radar_backscatter", arguments={
            "data": THIS,
            "elevation_model": elevation_model,
            "contributing_area": contributing_area,
            "ellipsoid_incidence_angle": ellipsoid_incidence_angle,
            "noise_removal": noise_removal
        })

    @openeo_process
    def sar_backscatter(
            self,
            coefficient: Union[str, None] = "gamma0-terrain",
            elevation_model: Union[str, None] = None,
            mask: bool = False,
            contributing_area: bool = False,
            local_incidence_angle: bool = False,
            ellipsoid_incidence_angle: bool = False,
            noise_removal: bool = True,
            options: Optional[dict] = None
    ) -> "DataCube":
        """
        Computes backscatter from SAR input.

        Note that backscatter computation may require instrument specific metadata that is tightly coupled to the
        original SAR products. As a result, this process may only work in combination with loading data from
        specific collections, not with general data cubes.

        :param coefficient: Select the radiometric correction coefficient.
            The following options are available:

            - `"beta0"`: radar brightness
            - `"sigma0-ellipsoid"`: ground area computed with ellipsoid earth model
            - `"sigma0-terrain"`: ground area computed with terrain earth model
            - `"gamma0-ellipsoid"`: ground area computed with ellipsoid earth model in sensor line of sight
            - `"gamma0-terrain"`: ground area computed with terrain earth model in sensor line of sight (default)
            - `None`: non-normalized backscatter
        :param elevation_model: The digital elevation model to use. Set to `None` (the default) to allow
            the back-end to choose, which will improve portability, but reduce reproducibility.
        :param mask: If set to `true`, a data mask is added to the bands with the name `mask`.
            It indicates which values are valid (1), invalid (0) or contain no-data (null).
        :param contributing_area: If set to `true`, a DEM-based local contributing area band named `contributing_area`
            is added. The values are given in square meters.
        :param local_incidence_angle: If set to `true`, a DEM-based local incidence angle band named
            `local_incidence_angle` is added. The values are given in degrees.
        :param ellipsoid_incidence_angle: If set to `true`, an ellipsoidal incidence angle band named
            `ellipsoid_incidence_angle` is added. The values are given in degrees.
        :param noise_removal: If set to `false`, no noise removal is applied. Defaults to `true`, which removes noise.
        :param options: dictionary with additional (backend-specific) options.
        :return:

        .. versionadded:: 0.4.9
        .. versionchanged:: 0.4.10 replace `orthorectify` and `rtc` arguments with `coefficient`.
        """
        coefficient_options = [
            "beta0", "sigma0-ellipsoid", "sigma0-terrain", "gamma0-ellipsoid", "gamma0-terrain", None
        ]
        if coefficient not in coefficient_options:
            raise OpenEoClientException("Invalid `sar_backscatter` coefficient {c!r}. Should be one of {o}".format(
                c=coefficient, o=coefficient_options
            ))
        arguments = {
            "data": THIS,
            "coefficient": coefficient,
            "elevation_model": elevation_model,
            "mask": mask,
            "contributing_area": contributing_area,
            "local_incidence_angle": local_incidence_angle,
            "ellipsoid_incidence_angle": ellipsoid_incidence_angle,
            "noise_removal": noise_removal,
        }
        if options:
            arguments["options"] = options
        return self.process(process_id="sar_backscatter", arguments=arguments)

    @openeo_process
    def fit_curve(self, parameters: list, function: Union[str, PGNode, typing.Callable], dimension: str):
        """
        Use non-linear least squares to fit a model function `y = f(x, parameters)` to data.

        The process throws an `InvalidValues` exception if invalid values are encountered.
        Invalid values are finite numbers (see also ``is_valid()``).

        .. warning:: experimental process: not generally supported, API subject to change.
            https://github.com/Open-EO/openeo-processes/pull/240

        :param parameters:
        :param function: "child callback" function, see :ref:`callbackfunctions`
        :param dimension:
        """
        return self.process(process_id="fit_curve", arguments={
            "data": THIS,
            "parameters": parameters,
            "function": self._get_callback(function, parent_parameters=["x", "parameters"]),
            "dimension": dimension
        })

    @openeo_process
    def predict_curve(
            self, parameters: list, function: Union[str, PGNode, typing.Callable], dimension: str,
            labels=None
    ):
        """
        Predict values using a model function and pre-computed parameters.

        .. warning:: experimental process: not generally supported, API subject to change.
            https://github.com/Open-EO/openeo-processes/pull/240

        :param parameters:
        :param function: "child callback" function, see :ref:`callbackfunctions`
        :param dimension:
        """
        return self.process(process_id="predict_curve", arguments={
            "data": THIS,
            "parameters": parameters,
            "function": self._get_callback(function, parent_parameters=["x", "parameters"]),
            "dimension": dimension,
            "labels": labels
        })

    @openeo_process(mode="reduce_dimension")
    def predict_random_forest(self, model: Union[str, BatchJob, MlModel], dimension: str = "bands"):
        """
        Apply ``reduce_dimension`` process with a ``predict_random_forest`` reducer.

        :param model: a reference to a trained model, one of

                - a :py:class:`~openeo.rest.mlmodel.MlModel` instance (e.g. loaded from :py:meth:`Connection.load_ml_model`)
                - a :py:class:`~openeo.rest.job.BatchJob` instance of a batch job that saved a single random forest model
                - a job id (``str``) of a batch job that saved a single random forest model
                - a STAC item URL (``str``) to load the random forest from.
                  (The STAC Item must implement the `ml-model` extension.)
        :param dimension: dimension along which to apply the ``reduce_dimension`` process.

        .. versionadded:: 0.10.0
        """
        if not isinstance(model, MlModel):
            model = MlModel.load_ml_model(connection=self.connection, id=model)
        from openeo.processes import predict_random_forest
        reducer = lambda data, context: predict_random_forest(data=data, model=context)
        return self.reduce_dimension(dimension=dimension, reducer=reducer, context=model)

    @openeo_process
    def dimension_labels(self, dimension: str) -> "DataCube":
        """
        Gives all labels for a dimension in the data cube. The labels have the same order as in the data cube.

        :param dimension: The name of the dimension to get the labels for.
        """
        dimension_names = self.metadata.dimension_names()
        if dimension_names and dimension not in dimension_names:
            raise ValueError(f"Invalid dimension name {dimension!r}, should be one of {dimension_names}")
        return self.process(process_id="dimension_labels", arguments={"data": THIS, "dimension": dimension})

    @openeo_process
    def fit_class_random_forest(
            self,
            # TODO #279 #293: target type should be `VectorCube` (with adapters for GeoJSON FeatureCollection, GeoPandas, ...)
            target: dict,
            # TODO #293 max_variables officially has no default
            max_variables: Optional[int] = None,
            num_trees: int = 100,
            seed: Optional[int] = None,
    ) -> 'MlModel':
        """
        Executes the fit of a random forest classification based on the user input of target and predictors.
        The Random Forest classification model is based on the approach by Breiman (2001).

        .. warning:: EXPERIMENTAL: not generally supported, API subject to change.

        :param target: The training sites for the classification model as a vector data cube. This is associated with the target
            variable for the Random Forest model. The geometry has to be associated with a value to predict (e.g. fractional
            forest canopy cover).
        :param max_variables: Specifies how many split variables will be used at a node. Default value is `null`, which corresponds to the
            number of predictors divided by 3.
        :param num_trees: The number of trees build within the Random Forest classification.
        :param seed: A randomization seed to use for the random sampling in training.

        .. versionadded:: 0.10.0
        """
        # TODO #279: `fit_class_random_forest` should be defined on VectorCube instead of DataCube
        pgnode = PGNode(
            process_id="fit_class_random_forest",
            arguments=dict_no_none(
                predictors=self,
                # TODO #279 strictly per-spec, target should be a `vector-cube`, but due to lack of proper support we are limited to inline GeoJSON for now
                target=target,
                max_variables=max_variables,
                num_trees=num_trees,
                seed=seed,
            ),
        )
        model = MlModel(graph=pgnode, connection=self._connection)
        return model

    @openeo_process
    def fit_regr_random_forest(
            self,
            # TODO #279 #293: target type should be `VectorCube` (with adapters for GeoJSON FeatureCollection, GeoPandas, ...)
            target: dict,
            # TODO #293 max_variables officially has no default
            max_variables: Optional[int] = None,
            num_trees: int = 100,
            seed: Optional[int] = None,
    ) -> 'MlModel':
        """
        Executes the fit of a random forest regression based on training data.
        The Random Forest regression model is based on the approach by Breiman (2001).

        .. warning:: EXPERIMENTAL: not generally supported, API subject to change.

        :param target: The training sites for the regression model as a vector data cube.
            This is associated with the target variable for the Random Forest model.
            The geometry has to associated with a value to predict (e.g. fractional forest canopy cover).
        :param max_variables: Specifies how many split variables will be used at a node. Default value is `null`, which corresponds to the
            number of predictors divided by 3.
        :param num_trees: The number of trees build within the Random Forest classification.
        :param seed: A randomization seed to use for the random sampling in training.

        .. versionadded:: 0.10.1
        """
        # TODO #279 #293: `fit_class_random_forest` should be defined on VectorCube instead of DataCube
        pgnode = PGNode(
            process_id="fit_regr_random_forest",
            arguments=dict_no_none(
                predictors=self,
                # TODO #279 strictly per-spec, target should be a `vector-cube`, but due to lack of proper support we are limited to inline GeoJSON for now
                target=target,
                max_variables=max_variables,
                num_trees=num_trees,
                seed=seed,
            ),
        )
        model = MlModel(graph=pgnode, connection=self._connection)
        return model

    @openeo_process
    def flatten_dimensions(self, dimensions: List[str], target_dimension: str, label_separator: Optional[str] = None):
        """
        Combines multiple given dimensions into a single dimension by flattening the values
        and merging the dimension labels with the given `label_separator`. Non-string dimension labels will
        be converted to strings. This process is the opposite of the process :py:meth:`unflatten_dimension()`
        but executing both processes subsequently doesn't necessarily create a data cube that
        is equal to the original data cube.

        :param dimensions: The names of the dimension to combine.
        :param target_dimension: The name of a target dimension with a single dimension label to replace.
        :param label_separator: The string that will be used as a separator for the concatenated dimension labels.
        :return: A data cube with the new shape.

        .. warning:: experimental process: not generally supported, API subject to change.
        .. versionadded:: 0.10.0
        """
        return self.process(
            process_id="flatten_dimensions",
            arguments=dict_no_none(
                data=THIS,
                dimensions=dimensions,
                target_dimension=target_dimension,
                label_separator=label_separator,
            ),
        )

    @openeo_process
    def unflatten_dimension(self, dimension: str, target_dimensions: List[str], label_separator: Optional[str] = None):
        """
        Splits a single dimension into multiple dimensions by systematically extracting values and splitting
        the dimension labels by the given `label_separator`.
        This process is the opposite of the process :py:meth:`flatten_dimensions()` but executing both processes
        subsequently doesn't necessarily create a data cube that is equal to the original data cube.

        :param dimension: The name of the dimension to split.
        :param target_dimensions: The names of the target dimensions.
        :param label_separator: The string that will be used as a separator to split the dimension labels.
        :return: A data cube with the new shape.

        .. warning:: experimental process: not generally supported, API subject to change.
        .. versionadded:: 0.10.0
        """
        return self.process(
            process_id="unflatten_dimension",
            arguments=dict_no_none(
                data=THIS,
                dimension=dimension,
                target_dimensions=target_dimensions,
                label_separator=label_separator,
            ),
        )

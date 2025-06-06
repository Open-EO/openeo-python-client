"""
The main module for creating earth observation processes. It aims to easily build complex process chains, that can
be evaluated by an openEO backend.

.. data:: THIS

    Symbolic reference to the current data cube, to be used as argument in :py:meth:`DataCube.process()` calls

"""
from __future__ import annotations

import datetime
import logging
import pathlib
import re
import typing
import urllib.parse
import warnings
from builtins import staticmethod
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import numpy as np
import requests
import shapely.geometry
import shapely.geometry.base
from shapely.geometry import MultiPolygon, Polygon, mapping

from openeo.api.process import Parameter, schema_supports
from openeo.dates import get_temporal_extent
from openeo.internal.documentation import openeo_process
from openeo.internal.graph_building import PGNode, ReduceNode, _FromNodeMixin
from openeo.internal.jupyter import in_jupyter_context
from openeo.internal.processes.builder import (
    ProcessBuilderBase,
    convert_callable_to_pgnode,
    get_parameter_names,
)
from openeo.internal.processes.parse import Process
from openeo.internal.warnings import UserDeprecationWarning, deprecated, legacy_alias
from openeo.metadata import (
    Band,
    BandDimension,
    CollectionMetadata,
    CubeMetadata,
    SpatialDimension,
    TemporalDimension,
    metadata_from_stac,
)
from openeo.processes import ProcessBuilder
from openeo.rest import (
    DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
    DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
    BandMathException,
    OpenEoClientException,
    OperatorException,
)
from openeo.rest._datacube import (
    THIS,
    UDF,
    _ProcessGraphAbstraction,
    build_child_callback,
)
from openeo.rest.graph_building import CollectionProperty
from openeo.rest.job import BatchJob, RESTJob
from openeo.rest.mlmodel import MlModel
from openeo.rest.models.general import ValidationResponse
from openeo.rest.result import SaveResult
from openeo.rest.service import Service
from openeo.rest.udp import RESTUserDefinedProcess
from openeo.rest.vectorcube import VectorCube
from openeo.util import dict_no_none, guess_format, load_json, normalize_crs, rfc3339

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    import xarray

    from openeo.rest.connection import Connection
    from openeo.udf import XarrayDataCube


# Sentinel value for arguments that are unset (when `None` has a different meaning)
_UNSET = object()


log = logging.getLogger(__name__)


# Type annotation aliases
InputDate = Union[str, datetime.date, Parameter, PGNode, ProcessBuilderBase, None]


class DataCube(_ProcessGraphAbstraction):
    """
    Class representing a openEO (raster) data cube.

    The data cube is represented by its corresponding openeo "process graph"
    and this process graph can be "grown" to a desired workflow by calling the appropriate methods.
    """

    # TODO: set this based on back-end or user preference?
    _DEFAULT_RASTER_FORMAT = "GTiff"

    def __init__(
        self, graph: PGNode, connection: Optional[Connection] = None, metadata: Optional[CollectionMetadata] = None
    ):
        super().__init__(pgnode=graph, connection=connection)
        self.metadata: Optional[CollectionMetadata] = metadata

    def process(
        self,
        process_id: str,
        arguments: Optional[dict] = None,
        metadata: Optional[CubeMetadata] = _UNSET,
        namespace: Optional[str] = None,
        **kwargs,
    ) -> DataCube:
        """
        Generic helper to create a new DataCube by applying a process.

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :param metadata: optional: metadata to override original cube metadata (e.g. when reducing dimensions)
        :param namespace: optional: process namespace
        :return: new DataCube instance
        """
        pg = self._build_pgnode(process_id=process_id, arguments=arguments, namespace=namespace, **kwargs)
        return DataCube(
            graph=pg,
            connection=self._connection,
            metadata=self.metadata if metadata is _UNSET else metadata,
        )

    graph_add_node = legacy_alias(process, "graph_add_node", since="0.1.1")

    def process_with_node(self, pg: PGNode, metadata: Optional[CollectionMetadata] = None) -> DataCube:
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

    def _do_metadata_normalization(self) -> bool:
        """Do metadata-based normalization/validation of dimension names, band names, ..."""
        return isinstance(self.metadata, CubeMetadata)

    def _assert_valid_dimension_name(self, name: str) -> str:
        if self._do_metadata_normalization():
            self.metadata.assert_valid_dimension(name)
        return name

    @classmethod
    @openeo_process
    def load_collection(
        cls,
        collection_id: Union[str, Parameter],
        connection: Optional[Connection] = None,
        spatial_extent: Union[dict, Parameter, shapely.geometry.base.BaseGeometry, str, pathlib.Path, None] = None,
        temporal_extent: Union[Sequence[InputDate], Parameter, str, None] = None,
        bands: Union[Iterable[str], Parameter, str, None] = None,
        fetch_metadata: bool = True,
        properties: Union[
            Dict[str, Union[PGNode, typing.Callable]], List[CollectionProperty], CollectionProperty, None
        ] = None,
        max_cloud_cover: Optional[float] = None,
    ) -> DataCube:
        """
        Create a new Raster Data cube.

        :param collection_id: image collection identifier
        :param connection: The backend connection to use.
            Can be ``None`` to work without connection and collection metadata.
        :param spatial_extent: limit data to specified bounding box or polygons. Can be provided in different ways:

            - a bounding box dictionary
            - a Shapely geometry object
            - a GeoJSON-style dictionary
            - a path (as :py:class:`str` or :py:class:`~pathlib.Path`) to a local, client-side GeoJSON file,
              which will be loaded automatically to get the geometries as GeoJSON construct.
            - a URL to a publicly accessible GeoJSON document
            - a :py:class:`~openeo.api.process.Parameter` instance.

        :param temporal_extent: limit data to specified temporal interval.
            Typically, just a two-item list or tuple containing start and end date.
            See :ref:`filtering-on-temporal-extent-section` for more details on temporal extent handling and shorthand notation.
        :param bands: only add the specified bands.
        :param properties: limit data by metadata property predicates.
            See :py:func:`~openeo.rest.graph_building.collection_property` for easy construction of such predicates.
        :param max_cloud_cover: shortcut to set maximum cloud cover ("eo:cloud_cover" collection property)
        :return: new DataCube containing the collection

        .. versionchanged:: 0.13.0
            added the ``max_cloud_cover`` argument.

        .. versionchanged:: 0.23.0
            Argument ``temporal_extent``: add support for year/month shorthand notation
            as discussed at :ref:`date-shorthand-handling`.

        .. versionchanged:: 0.26.0
            Add :py:func:`~openeo.rest.graph_building.collection_property` support to ``properties`` argument.

        .. versionchanged:: 0.37.0
            Argument ``spatial_extent``: add support for passing a Shapely geometry or a local path to a GeoJSON file.
        """
        if temporal_extent:
            temporal_extent = cls._get_temporal_extent(extent=temporal_extent)
        spatial_extent = _get_geometry_argument(
            argument=spatial_extent,
            valid_geojson_types=["Polygon", "MultiPolygon", "Feature", "FeatureCollection"],
            connection=connection,
            allow_none=True,
            allow_parameter=True,
            allow_bounding_box=True,
            argument_name="spatial_extent",
            process_id="load_collection",
        )

        arguments = {
            'id': collection_id,
            'spatial_extent': spatial_extent,
            'temporal_extent': temporal_extent,
        }
        if isinstance(collection_id, Parameter):
            fetch_metadata = False
        metadata: Optional[CollectionMetadata] = (
            connection.collection_metadata(collection_id) if connection and fetch_metadata else None
        )
        if bands is not None:
            bands = cls._get_bands(bands, process_id="load_collection")
            if isinstance(bands, Parameter):
                metadata = None
            if metadata:
                bands = [b if isinstance(b, str) else metadata.band_dimension.band_name(b) for b in bands]
                # TODO: also apply spatial/temporal filters to metadata?
                metadata = metadata.filter_bands(bands)
            arguments['bands'] = bands

        properties = cls._build_load_properties_argument(
            properties=properties,
            supported_properties=(metadata.get("summaries", default={}).keys() if metadata else None),
            max_cloud_cover=max_cloud_cover,
        )
        if properties is not None:
            arguments["properties"] = properties

        pg = PGNode(
            process_id='load_collection',
            arguments=arguments
        )
        return cls(graph=pg, connection=connection, metadata=metadata)

    create_collection = legacy_alias(
        load_collection, name="create_collection", since="0.4.6"
    )

    @classmethod
    def _build_load_properties_argument(
        cls,
        properties: Union[
            Dict[str, Union[PGNode, typing.Callable]],
            List[CollectionProperty],
            CollectionProperty,
            None,
        ],
        *,
        supported_properties: Optional[typing.Collection[str]] = None,
        max_cloud_cover: Optional[float] = None,
    ) -> Union[Dict[str, PGNode], None]:
        """
        Helper to convert/build the ``properties`` argument
        for ``load_collection`` and ``load_stac`` from user input
        """
        if isinstance(properties, CollectionProperty):
            properties = [properties]
        if isinstance(properties, list):
            if not all(isinstance(p, CollectionProperty) for p in properties):
                raise ValueError(
                    f"When providing load properties as a list, all items must be CollectionProperty objects, but got {properties}"
                )
            properties = {p.name: p.from_node() for p in properties}

        if max_cloud_cover is not None:
            properties = properties or {}
            properties["eo:cloud_cover"] = lambda v: v <= max_cloud_cover

        if isinstance(properties, dict):
            if supported_properties:
                unsupported_properties = set(properties.keys()).difference(supported_properties)
                if unsupported_properties:
                    warnings.warn(
                        f"Property filtering with unsupported properties according to collection/STAC metadata: {unsupported_properties} (supported: {supported_properties}).",
                        stacklevel=3,
                    )
            properties = {
                prop: build_child_callback(pred, parent_parameters=["value"]) for prop, pred in properties.items()
            }

        return properties

    @classmethod
    @deprecated(reason="Depends on non-standard process, replace with :py:meth:`openeo.rest.connection.Connection.load_stac` where possible.",version="0.25.0")
    def load_disk_collection(cls, connection: Connection, file_format: str, glob_pattern: str, **options) -> DataCube:
        """
        Loads image data from disk as a DataCube.
        This is backed by a non-standard process ('load_disk_data'). This will eventually be replaced by standard options such as
        :py:meth:`openeo.rest.connection.Connection.load_stac` or https://processes.openeo.org/#load_uploaded_files


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
        return cls(graph=pg, connection=connection)

    @classmethod
    def load_stac(
        cls,
        url: str,
        spatial_extent: Union[dict, Parameter, shapely.geometry.base.BaseGeometry, str, pathlib.Path, None] = None,
        temporal_extent: Union[Sequence[InputDate], Parameter, str, None] = None,
        bands: Union[Iterable[str], Parameter, str, None] = None,
        properties: Union[
            Dict[str, Union[PGNode, typing.Callable]], List[CollectionProperty], CollectionProperty, None
        ] = None,
        connection: Optional[Connection] = None,
    ) -> DataCube:
        """
        Loads data from a static STAC catalog or a STAC API Collection and returns the data as a processable :py:class:`DataCube`.
        A batch job result can be loaded by providing a reference to it.

        If supported by the underlying metadata and file format, the data that is added to the data cube can be
        restricted with the parameters ``spatial_extent``, ``temporal_extent`` and ``bands``.
        If no data is available for the given extents, a ``NoDataAvailable`` error is thrown.

        Remarks:

        * The bands (and all dimensions that specify nominal dimension labels) are expected to be ordered as
          specified in the metadata if the ``bands`` parameter is set to ``null``.
        * If no additional parameter is specified this would imply that the whole data set is expected to be loaded.
          Due to the large size of many data sets, this is not recommended and may be optimized by back-ends to only
          load the data that is actually required after evaluating subsequent processes such as filters.
          This means that the values should be processed only after the data has been limited to the required extent
          and as a consequence also to a manageable size.


        :param url: The URL to a static STAC catalog (STAC Item, STAC Collection, or STAC Catalog)
            or a specific STAC API Collection that allows to filter items and to download assets.
            This includes batch job results, which itself are compliant to STAC.
            For external URLs, authentication details such as API keys or tokens may need to be included in the URL.

            Batch job results can be specified in two ways:

            - For Batch job results at the same back-end, a URL pointing to the corresponding batch job results
              endpoint should be provided. The URL usually ends with ``/jobs/{id}/results`` and ``{id}``
              is the corresponding batch job ID.
            - For external results, a signed URL must be provided. Not all back-ends support signed URLs,
              which are provided as a link with the link relation `canonical` in the batch job result metadata.
        :param spatial_extent:
            Limits the data to load to the specified bounding box or polygons.

            For raster data, the process loads the pixel into the data cube if the point at the pixel center intersects
            with the bounding box or any of the polygons (as defined in the Simple Features standard by the OGC).

            For vector data, the process loads the geometry into the data cube if the geometry is fully within the
            bounding box or any of the polygons (as defined in the Simple Features standard by the OGC).
            Empty geometries may only be in the data cube if no spatial extent has been provided.

            The GeoJSON can be one of the following feature types:

            * A ``Polygon`` or ``MultiPolygon`` geometry,
            * a ``Feature`` with a ``Polygon`` or ``MultiPolygon`` geometry, or
            * a ``FeatureCollection`` containing at least one ``Feature`` with ``Polygon`` or ``MultiPolygon`` geometries.

            Set this parameter to ``None`` to set no limit for the spatial extent.
            Be careful with this when loading large datasets. It is recommended to use this parameter instead of
            using ``filter_bbox()`` or ``filter_spatial()`` directly after loading unbounded data.

        :param temporal_extent:
            Limits the data to load to the specified left-closed temporal interval.
            Applies to all temporal dimensions.
            The interval has to be specified as an array with exactly two elements:

            1.  The first element is the start of the temporal interval.
                The specified instance in time is **included** in the interval.
            2.  The second element is the end of the temporal interval.
                The specified instance in time is **excluded** from the interval.

            The second element must always be greater/later than the first element.
            Otherwise, a `TemporalExtentEmpty` exception is thrown.

            Also supports open intervals by setting one of the boundaries to ``None``, but never both.

            Set this parameter to ``None`` to set no limit for the temporal extent.
            Be careful with this when loading large datasets. It is recommended to use this parameter instead of
            using ``filter_temporal()`` directly after loading unbounded data.

        :param bands:
            Only adds the specified bands into the data cube so that bands that don't match the list
            of band names are not available. Applies to all dimensions of type `bands`.

            Either the unique band name (metadata field ``name`` in bands) or one of the common band names
            (metadata field ``common_name`` in bands) can be specified.
            If the unique band name and the common name conflict, the unique band name has a higher priority.

            The order of the specified array defines the order of the bands in the data cube.
            If multiple bands match a common name, all matched bands are included in the original order.

            It is recommended to use this parameter instead of using ``filter_bands()`` directly after loading unbounded data.

        :param properties:
            Limits the data by metadata properties to include only data in the data cube which
            all given conditions return ``True`` for (AND operation).

            Specify key-value-pairs with the key being the name of the metadata property,
            which can be retrieved with the openEO Data Discovery for Collections.
            The value must be a condition (user-defined process) to be evaluated against a STAC API.
            This parameter is not supported for static STAC.

            See :py:func:`~openeo.rest.graph_building.collection_property` for easy construction of property filters.

        :param connection: The connection to use to connect with the backend.

        .. versionadded:: 0.33.0

        .. versionchanged:: 0.37.0
            Argument ``spatial_extent``: add support for passing a Shapely geometry or a local path to a GeoJSON file.

        .. versionchanged:: 0.41.0
            Add support for :py:func:`~openeo.rest.graph_building.collection_property` based property filters
        """
        arguments: dict = {"url": url}
        if spatial_extent:
            arguments["spatial_extent"] = _get_geometry_argument(
                argument=spatial_extent,
                valid_geojson_types=["Polygon", "MultiPolygon", "Feature", "FeatureCollection"],
                connection=connection,
                allow_none=True,
                allow_parameter=True,
                allow_bounding_box=True,
                argument_name="spatial_extent",
                process_id="load_stac",
            )

        if temporal_extent:
            arguments["temporal_extent"] = DataCube._get_temporal_extent(extent=temporal_extent)
        bands = cls._get_bands(bands, process_id="load_stac")
        if bands is not None:
            arguments["bands"] = bands

        properties = cls._build_load_properties_argument(properties=properties)
        if properties is not None:
            arguments["properties"] = properties

        graph = PGNode("load_stac", arguments=arguments)
        try:
            metadata = metadata_from_stac(url)
            # TODO: also apply spatial/temporal filters to metadata?

            if isinstance(bands, list):
                if metadata.has_band_dimension():
                    unknown_bands = [b for b in bands if not metadata.band_dimension.contains_band(b)]
                    if len(unknown_bands) == 0:
                        # Ideal case: bands requested by user correspond with bands extracted from metadata.
                        metadata = metadata.filter_bands(band_names=bands)
                    else:
                        metadata = metadata._ensure_band_dimension(
                            bands=bands,
                            warning=f"The specified bands {bands} in `load_stac` are not a subset of the bands {metadata.band_dimension.band_names} found in the STAC metadata (unknown bands: {unknown_bands}). Working with specified bands as is.",
                        )
                else:
                    metadata = metadata._ensure_band_dimension(
                        name="bands",
                        bands=bands,
                        warning=f"Bands {bands} were specified in `load_stac`, but no band dimension was detected in the STAC metadata. Working with band dimension and specified bands.",
                    )

        except Exception as e:
            log.warning(f"Failed to extract cube metadata from STAC URL {url}", exc_info=True)
            metadata = None
        return cls(graph=graph, connection=connection, metadata=metadata)

    @classmethod
    def _get_temporal_extent(
        cls,
        *args,
        start_date: InputDate = None,
        end_date: InputDate = None,
        extent: Union[Sequence[InputDate], Parameter, str, None] = None,
    ) -> Union[List[Union[str, Parameter, PGNode, None]], Parameter]:
        """Parameter aware temporal_extent normalizer"""
        # TODO: move this outside of DataCube class
        # TODO: return extent as tuple instead of list
        if len(args) == 1 and isinstance(args[0], Parameter):
            assert start_date is None and end_date is None and extent is None
            return args[0]
        elif len(args) == 0 and isinstance(extent, Parameter):
            assert start_date is None and end_date is None
            # TODO: warn about unexpected parameter schema
            return extent
        else:
            def convertor(d: Any) -> Any:
                # TODO: can this be generalized through _FromNodeMixin?
                if isinstance(d, Parameter) or isinstance(d, PGNode):
                    # TODO: warn about unexpected parameter schema
                    return d
                elif isinstance(d, ProcessBuilderBase):
                    return d.pgnode
                else:
                    return rfc3339.normalize(d)

            return list(
                get_temporal_extent(*args, start_date=start_date, end_date=end_date, extent=extent, convertor=convertor)
            )

    @staticmethod
    def _get_bands(
        bands: Union[Iterable[str], Parameter, str, None], process_id: str
    ) -> Union[None, List[str], Parameter]:
        """Normalize band array for processes like load_collection, load_stac"""
        if bands is None:
            pass
        elif isinstance(bands, str):
            bands = [bands]
        elif isinstance(bands, Parameter):
            pass
        else:
            # Coerce to list
            bands = list(bands)
            if len(bands) == 0:
                raise OpenEoClientException(f"Bands array should not be empty (process {process_id!r})")
        return bands

    @openeo_process
    def filter_temporal(
        self,
        *args,
        start_date: InputDate = None,
        end_date: InputDate = None,
        extent: Union[Sequence[InputDate], Parameter, str, None] = None,
    ) -> DataCube:
        """
        Limit the DataCube to a certain date range, which can be specified in several ways:

        >>> cube.filter_temporal("2019-07-01", "2019-08-01")
        >>> cube.filter_temporal(["2019-07-01", "2019-08-01"])
        >>> cube.filter_temporal(extent=["2019-07-01", "2019-08-01"])
        >>> cube.filter_temporal(start_date="2019-07-01", end_date="2019-08-01"])

        See :ref:`filtering-on-temporal-extent-section` for more details on temporal extent handling and shorthand notation.

        :param start_date: start date of the filter (inclusive), as a string or date object
        :param end_date: end date of the filter (exclusive), as a string or date object
        :param extent: temporal extent.
            Typically, specified as a two-item list or tuple containing start and end date.

        .. versionchanged:: 0.23.0
            Arguments ``start_date``, ``end_date`` and ``extent``:
            add support for year/month shorthand notation as discussed at :ref:`date-shorthand-handling`.
        """
        if len(args) == 1 and isinstance(args[0], (str)):
            raise OpenEoClientException(
                f"filter_temporal() with a single string argument ({args[0]!r}) is ambiguous."
                f" If you want a half-unbounded interval, use something like filter_temporal({args[0]!r}, None) or use explicit keyword arguments."
                f" If you want the full interval covering all of {args[0]!r}, use something like filter_temporal(extent={args[0]!r})."
            )
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
        west: Optional[float] = None,
        south: Optional[float] = None,
        east: Optional[float] = None,
        north: Optional[float] = None,
        crs: Optional[Union[int, str]] = None,
        base: Optional[float] = None,
        height: Optional[float] = None,
        bbox: Union[Sequence[float], Parameter, None] = None,
    ) -> DataCube:
        """
        Limits the data cube to the specified bounding box.

        The bounding box can be specified in multiple ways.

            - With keyword arguments::

                >>> cube.filter_bbox(west=3, south=51, east=4, north=52, crs=4326)

            - With a (west, south, east, north) list or tuple
              (note that EPSG:4326 is the default CRS, so it's not necessary to specify it explicitly)::

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

                >>> cube.filter_bbox(
                ... west=652000, east=672000, north=5161000, south=5181000,
                ... crs=32632
                ... )

            - Deprecated: positional arguments are also supported,
              but follow a non-standard order for legacy reasons::

                >>> west, east, north, south = 3, 4, 52, 51
                >>> cube.filter_bbox(west, east, north, south)

        :param crs: value describing the coordinate reference system.
            Typically just an int (interpreted as EPSG code, e.g. ``4326``)
            or a string (handled as authority string, e.g. ``"EPSG:4326"``).
            See :py:func:`openeo.util.normalize_crs` for more details about additional normalization that is applied to this argument.
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
                    crs = normalize_crs(args[4])
            elif len(args) == 1 and (isinstance(args[0], (list, tuple)) and len(args[0]) == 4
                                     or isinstance(args[0], (dict, shapely.geometry.base.BaseGeometry, Parameter))):
                bbox = args[0]
            else:
                raise ValueError(args)

        if isinstance(bbox, Parameter):
            if not schema_supports(bbox.schema, type="object"):
                warnings.warn(
                    "Unexpected parameterized `extent` in `filter_bbox`:"
                    f" expected schema compatible with type 'object' but got {bbox.schema!r}."
                )
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
        self, geometries: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, VectorCube]
    ) -> DataCube:
        """
        Limits the data cube over the spatial dimensions to the specified geometries.

            - For polygons, the filter retains a pixel in the data cube if the point at the pixel center intersects with
              at least one of the polygons (as defined in the Simple Features standard by the OGC).
            - For points, the process considers the closest pixel center.
            - For lines (line strings), the process considers all the pixels whose centers are closest to at least one
              point on the line.

        More specifically, pixels outside of the bounding box of the given geometry will not be available after filtering.
        All pixels inside the bounding box that are not retained will be set to null (no data).

        :param geometries: One or more geometries used for filtering, Can be provided in different ways:

            - a shapely geometry
            - a GeoJSON-style dictionary,
            - a public URL to the geometries in a vector format that is supported by the backend
              (also see :py:func:`Connection.list_file_formats() <openeo.rest.connection.Connection.list_file_formats>`),
              e.g. GeoJSON, GeoParquet, etc.
              A ``load_url`` process will automatically be added to the process graph.
            - a path (:py:class:`str` or :py:class:`~pathlib.Path`) to a local, client-side GeoJSON file,
              which will be loaded automatically to get the geometries as GeoJSON construct.
            - a :py:class:`~openeo.rest.vectorcube.VectorCube` instance.
            - a :py:class:`~openeo.api.process.Parameter` instance.

        :return: A data cube restricted to the specified geometries. The dimensions and dimension properties (name,
            type, labels, reference system and resolution) remain unchanged, except that the spatial dimensions have less
            (or the same) dimension labels.

        .. versionchanged:: 0.36.0
            Support passing a URL as ``geometries`` argument, which will be loaded with the ``load_url`` process.

        .. versionchanged:: 0.36.0
            Support for passing a backend-side path as ``geometries`` argument was removed
            (also see :ref:`legacy_read_vector`).
            Instead, it's possible to provide a client-side path to a GeoJSON file
            (which will be loaded client-side to get the geometries as GeoJSON construct).
        """
        valid_geojson_types = [
            "Point",
            "MultiPoint",
            "LineString",
            "MultiLineString",
            "Polygon",
            "MultiPolygon",
            "GeometryCollection",  # TODO #706 stop allowing GeometryCollection
            "FeatureCollection",
        ]
        geometries = _get_geometry_argument(geometries, valid_geojson_types=valid_geojson_types, connection=self.connection, crs=None)
        return self.process(
            process_id='filter_spatial',
            arguments={
                'data': THIS,
                'geometries': geometries
            }
        )

    @openeo_process
    def filter_bands(self, bands: Union[List[Union[str, int]], str]) -> DataCube:
        """
        Filter the data cube by the given bands

        :param bands: list of band names, common names or band indices. Single band name can also be given as string.
        :return: a DataCube instance
        """
        if isinstance(bands, str):
            bands = [bands]
        if self._do_metadata_normalization():
            bands = [self.metadata.band_dimension.band_name(b) for b in bands]
        cube = self.process(
            process_id="filter_bands",
            arguments={"data": THIS, "bands": bands},
            metadata=self.metadata.filter_bands(bands) if self.metadata else None,
        )
        return cube

    @openeo_process
    def filter_labels(
        self, condition: Union[PGNode, Callable], dimension: str, context: Optional[dict] = None
    ) -> DataCube:
        """
        Filters the dimension labels in the data cube for the given dimension.
        Only the dimension labels that match the specified condition are preserved,
        all other labels with their corresponding data get removed.

        :param condition: the "child callback" which will be given a single label value (number or string)
            and returns a boolean expressing if the label should be preserved.
            Also see :ref:`callbackfunctions`.
        :param dimension: The name of the dimension to filter on.

        .. versionadded:: 0.27.0
        """
        condition = build_child_callback(condition, parent_parameters=["value"])
        return self.process(
            process_id="filter_labels",
            arguments=dict_no_none(data=THIS, condition=condition, dimension=dimension, context=context),
        )

    band_filter = legacy_alias(filter_bands, "band_filter", since="0.1.0")

    def band(self, band: Union[str, int]) -> DataCube:
        """
        Filter out a single band

        :param band: band name, band common name or band index.
        :return: a DataCube instance
        """
        if self._do_metadata_normalization():
            band = self.metadata.band_dimension.band_index(band)
        arguments = {"data": {"from_parameter": "data"}}
        if isinstance(band, int):
            arguments["index"] = band
        else:
            arguments["label"] = band
        return self.reduce_bands(reducer=PGNode(process_id="array_element", arguments=arguments))

    @openeo_process
    def resample_spatial(
        self,
        resolution: Union[float, Tuple[float, float], List[float]] = 0.0,
        projection: Union[int, str, None] = None,
        method: str = "near",
        align: str = "upper-left",
    ) -> DataCube:
        metadata = (self.metadata or CubeMetadata()).resample_spatial(resolution=resolution, projection=projection)
        return self.process(
            process_id="resample_spatial",
            arguments={
                "data": THIS,
                "resolution": resolution,
                "projection": projection,
                "method": method,
                "align": align,
            },
            metadata=metadata,
        )

    def resample_cube_spatial(self, target: DataCube, method: str = "near") -> DataCube:
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
        if target.metadata:
            metadata = (self.metadata or CubeMetadata()).resample_cube_spatial(target=target.metadata)
        else:
            metadata = None
        return self.process(
            process_id="resample_cube_spatial",
            arguments={"data": self, "target": target, "method": method},
            metadata=metadata,
        )

    @openeo_process
    def resample_cube_temporal(
        self, target: DataCube, dimension: Optional[str] = None, valid_within: Optional[int] = None
    ) -> DataCube:
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

    def _operator_binary(self, operator: str, other: Union[DataCube, int, float], reverse=False) -> DataCube:
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

    def _operator_unary(self, operator: str, **kwargs) -> DataCube:
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
    ) -> DataCube:
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
    def add(self, other: Union[DataCube, int, float], reverse=False) -> DataCube:
        return self._operator_binary("add", other, reverse=reverse)

    @openeo_process(mode="operator")
    def subtract(self, other: Union[DataCube, int, float], reverse=False) -> DataCube:
        return self._operator_binary("subtract", other, reverse=reverse)

    @openeo_process(mode="operator")
    def divide(self, other: Union[DataCube, int, float], reverse=False) -> DataCube:
        return self._operator_binary("divide", other, reverse=reverse)

    @openeo_process(mode="operator")
    def multiply(self, other: Union[DataCube, int, float], reverse=False) -> DataCube:
        return self._operator_binary("multiply", other, reverse=reverse)

    @openeo_process
    def normalized_difference(self, other: DataCube) -> DataCube:
        # This DataCube method is only a convenience function when in band math mode
        assert self._in_bandmath_mode()
        assert other._in_bandmath_mode()
        return self._operator_binary("normalized_difference", other)

    @openeo_process(process_id="or", mode="operator")
    def logical_or(self, other: DataCube) -> DataCube:
        """
        Apply element-wise logical `or` operation

        :param other:
        :return: logical_or(this, other)
        """
        return self._operator_binary("or", other)

    @openeo_process(process_id="and", mode="operator")
    def logical_and(self, other: DataCube) -> DataCube:
        """
        Apply element-wise logical `and` operation

        :param other:
        :return: logical_and(this, other)
        """
        return self._operator_binary("and", other)

    @openeo_process(process_id="not", mode="operator")
    def __invert__(self) -> DataCube:
        return self._operator_unary("not")

    @openeo_process(process_id="neq", mode="operator")
    def __ne__(self, other: Union[DataCube, int, float]) -> DataCube:
        return self._operator_binary("neq", other)

    @openeo_process(process_id="eq", mode="operator")
    def __eq__(self, other: Union[DataCube, int, float]) -> DataCube:
        """
        Pixelwise comparison of this data cube with another cube or constant.

        :param other: Another data cube, or a constant
        :return:
        """
        return self._operator_binary("eq", other)

    @openeo_process(process_id="gt", mode="operator")
    def __gt__(self, other: Union[DataCube, int, float]) -> DataCube:
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.

        :param other:
        :return: this > other
        """
        return self._operator_binary("gt", other)

    @openeo_process(process_id="ge", mode="operator")
    def __ge__(self, other: Union[DataCube, int, float]) -> DataCube:
        return self._operator_binary("gte", other)

    @openeo_process(process_id="lt", mode="operator")
    def __lt__(self, other: Union[DataCube, int, float]) -> DataCube:
        """
        Pairwise comparison of the bands in this data cube with the bands in the 'other' data cube.
        The number of bands in both data cubes has to be the same.

        :param other:
        :return: this < other
        """
        return self._operator_binary("lt", other)

    @openeo_process(process_id="le", mode="operator")
    def __le__(self, other: Union[DataCube, int, float]) -> DataCube:
        return self._operator_binary("lte", other)

    @openeo_process(process_id="add", mode="operator")
    def __add__(self, other) -> DataCube:
        return self.add(other)

    @openeo_process(process_id="add", mode="operator")
    def __radd__(self, other) -> DataCube:
        return self.add(other, reverse=True)

    @openeo_process(process_id="subtract", mode="operator")
    def __sub__(self, other) -> DataCube:
        return self.subtract(other)

    @openeo_process(process_id="subtract", mode="operator")
    def __rsub__(self, other) -> DataCube:
        return self.subtract(other, reverse=True)

    @openeo_process(process_id="multiply", mode="operator")
    def __neg__(self) -> DataCube:
        return self.multiply(-1)

    @openeo_process(process_id="multiply", mode="operator")
    def __mul__(self, other) -> DataCube:
        return self.multiply(other)

    @openeo_process(process_id="multiply", mode="operator")
    def __rmul__(self, other) -> DataCube:
        return self.multiply(other, reverse=True)

    @openeo_process(process_id="divide", mode="operator")
    def __truediv__(self, other) -> DataCube:
        return self.divide(other)

    @openeo_process(process_id="divide", mode="operator")
    def __rtruediv__(self, other) -> DataCube:
        return self.divide(other, reverse=True)

    @openeo_process(process_id="power", mode="operator")
    def __rpow__(self, other) -> DataCube:
        return self._power(other, reverse=True)

    @openeo_process(process_id="power", mode="operator")
    def __pow__(self, other) -> DataCube:
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
    def ln(self) -> DataCube:
        return self._operator_unary("ln")

    @openeo_process(process_id="log", mode="operator")
    def logarithm(self, base: float) -> DataCube:
        return self._operator_unary("log", base=base)

    @openeo_process(process_id="log", mode="operator")
    def log2(self) -> DataCube:
        return self.logarithm(base=2)

    @openeo_process(process_id="log", mode="operator")
    def log10(self) -> DataCube:
        return self.logarithm(base=10)

    @openeo_process(process_id="or", mode="operator")
    def __or__(self, other) -> DataCube:
        return self.logical_or(other)

    @openeo_process(process_id="and", mode="operator")
    def __and__(self, other):
        return self.logical_and(other)

    def _bandmath_operator_binary_cubes(
        self, operator, other: DataCube, left_arg_name="x", right_arg_name="y"
    ) -> DataCube:
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

    def _bandmath_operator_binary_scalar(self, operator: str, other: Union[int, float], reverse=False) -> DataCube:
        """Band math binary operator with scalar value (int or float) as right hand side argument"""
        node = self._get_bandmath_node()
        x = {'from_node': node.reducer_process_graph()}
        y = other
        if reverse:
            x, y = y, x
        return self.process_with_node(node.clone_with_new_reducer(
            PGNode(operator, x=x, y=y)
        ))

    def _bandmath_operator_unary(self, operator: str, **kwargs) -> DataCube:
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

    def _merge_operator_binary_cubes(
        self, operator: str, other: DataCube, left_arg_name="x", right_arg_name="y"
    ) -> DataCube:
        """Merge two cubes with given operator as overlap_resolver."""
        # TODO #123 reuse an existing merge_cubes process graph if it already exists?
        return self.merge_cubes(other, overlap_resolver=PGNode(
            process_id=operator,
            arguments={
                left_arg_name: {"from_parameter": "x"},
                right_arg_name: {"from_parameter": "y"},
            }
        ))

    @openeo_process
    def aggregate_spatial(
        self,
        geometries: Union[
            shapely.geometry.base.BaseGeometry,
            dict,
            str,
            pathlib.Path,
            Parameter,
            VectorCube,
        ],
        reducer: Union[str, typing.Callable, PGNode],
        target_dimension: Optional[str] = None,
        crs: Optional[Union[int, str]] = None,
        context: Optional[dict] = None,
        # TODO arguments: target dimension, context
    ) -> VectorCube:
        """
        Aggregates statistics for one or more geometries (e.g. zonal statistics for polygons)
        over the spatial dimensions.

        :param geometries: The geometries to aggregate in. Can be provided in different ways:

            - a shapely geometry
            - a GeoJSON-style dictionary,
            - a public URL to the geometries in a vector format that is supported by the backend
              (also see :py:func:`Connection.list_file_formats() <openeo.rest.connection.Connection.list_file_formats>`),
              e.g. GeoJSON, GeoParquet, etc.
              A ``load_url`` process will automatically be added to the process graph.
            - a path (:py:class:`str` or :py:class:`~pathlib.Path`) to a local, client-side GeoJSON file,
              which will be loaded automatically to get the geometries as GeoJSON construct.
            - a :py:class:`~openeo.rest.vectorcube.VectorCube` instance.
            - a :py:class:`~openeo.api.process.Parameter` instance.

        :param reducer: the "child callback":
            the name of a single openEO process,
            or a callback function as discussed in :ref:`callbackfunctions`,
            or a :py:class:`UDF <openeo.rest._datacube.UDF>` instance.

            The callback should correspond to a process that
            receives an array of numerical values
            and returns a single numerical value.
            For example:

            -   ``"mean"`` (string)
            -   :py:func:`absolute <openeo.processes.max>` (:ref:`predefined openEO process function <openeo_processes_functions>`)
            -   ``lambda data: data.min()`` (function or lambda)

        :param target_dimension: The new dimension name to be used for storing the results.
        :param crs: The spatial reference system of the provided polygon.
            By default, longitude-latitude (EPSG:4326) is assumed.
            See :py:func:`openeo.util.normalize_crs` for more details about additional normalization that is applied to this argument.

            .. note:: this ``crs`` argument is a non-standard/experimental feature, only supported by specific back-ends.
                See https://github.com/Open-EO/openeo-processes/issues/235 for details.

        :param context: Additional data to be passed to the reducer process.

        .. versionchanged:: 0.36.0
            Support passing a URL as ``geometries`` argument, which will be loaded with the ``load_url`` process.

        .. versionchanged:: 0.36.0
            Support for passing a backend-side path as ``geometries`` argument was removed
            (also see :ref:`legacy_read_vector`).
            Instead, it's possible to provide a client-side path to a GeoJSON file
            (which will be loaded client-side to get the geometries as GeoJSON construct).
        """
        valid_geojson_types = [
            "Point",
            "MultiPoint",
            "LineString",
            "MultiLineString",
            "Polygon",
            "MultiPolygon",
            "GeometryCollection",  # TODO #706 stop allowing GeometryCollection
            "Feature",
            "FeatureCollection",
        ]
        geometries = _get_geometry_argument(geometries, valid_geojson_types=valid_geojson_types, connection= self.connection, crs=crs)
        reducer = build_child_callback(reducer, parent_parameters=["data"])
        return VectorCube(
            graph=self._build_pgnode(
                process_id="aggregate_spatial",
                data=THIS,
                geometries=geometries,
                reducer=reducer,
                arguments=dict_no_none(
                    target_dimension=target_dimension, context=context
                ),
            ),
            connection=self._connection,
            # TODO: also add new "geometry" dimension #457
            metadata=None if self.metadata is None else self.metadata.reduce_spatial(),
        )

    @openeo_process
    def aggregate_spatial_window(
        self,
        reducer: Union[str, typing.Callable, PGNode],
        size: List[int],
        boundary: str = "pad",
        align: str = "upper-left",
        context: Optional[dict] = None,
        # TODO arguments: target dimension, context
    ) -> DataCube:
        """
        Aggregates statistics over the horizontal spatial dimensions (axes x and y) of the data cube.

        The pixel grid for the axes x and y is divided into non-overlapping windows with the size
        specified in the parameter size. If the number of values for the axes x and y is not a multiple
        of the corresponding window size, the behavior specified in the parameters boundary and align
        is applied. For each of these windows, the reducer process computes the result.

        :param reducer: the "child callback":
            the name of a single openEO process,
            or a callback function as discussed in :ref:`callbackfunctions`,
            or a :py:class:`UDF <openeo.rest._datacube.UDF>` instance.
        :param size: Window size in pixels along the horizontal spatial dimensions.
            The first value corresponds to the x axis, the second value corresponds to the y axis.
        :param boundary: Behavior to apply if the number of values for the axes x and y is not a
            multiple of the corresponding value in the size parameter.
            Options are:

                - ``pad`` (default): pad the data cube with the no-data value null to fit the required window size.
                - ``trim``: trim the data cube to fit the required window size.

            Use the parameter ``align`` to align the data to the desired corner.

        :param align: If the data requires padding or trimming (see parameter ``boundary``), specifies
            to which corner of the spatial extent the data is aligned to. For example, if the data is
            aligned to the upper left, the process pads/trims at the lower-right.
        :param context: Additional data to be passed to the process.

        :return: A data cube with the newly computed values and the same dimensions.
        """
        valid_boundary_types = ["pad", "trim"]
        valid_align_types = ["lower-left", "upper-left", "lower-right", "upper-right"]
        if boundary not in valid_boundary_types:
            raise ValueError(f"Provided boundary type not supported. Please use one of {valid_boundary_types} .")
        if align not in valid_align_types:
            raise ValueError(f"Provided align type not supported. Please use one of {valid_align_types} .")
        if len(size) != 2:
            raise ValueError(f"Provided size not supported. Please provide a list of 2 integer values.")

        reducer = build_child_callback(reducer, parent_parameters=["data"])
        arguments = {
            "data": THIS,
            "boundary": boundary,
            "align": align,
            "size": size,
            "reducer": reducer,
            "context": context,
        }
        return self.process(process_id="aggregate_spatial_window", arguments=arguments)

    @openeo_process
    def apply_dimension(
        self,
        code: Optional[str] = None,
        runtime=None,
        # TODO: drop None default of process (when `code` and `runtime` args can be dropped)
        process: Union[str, typing.Callable, UDF, PGNode] = None,
        version: Optional[str] = None,
        # TODO #774 fully drop default value for dimension after deprecation period
        dimension: str = _UNSET,
        target_dimension: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> DataCube:
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

        :param code: [**deprecated**] UDF code or process identifier (optional)
        :param runtime: [**deprecated**] UDF runtime to use (optional)
        :param process: the "child callback":
            the name of a single process,
            or a callback function as discussed in :ref:`callbackfunctions`,
            or a :py:class:`UDF <openeo.rest._datacube.UDF>` instance.

            The callback should correspond to a process that
            receives an array of numerical values
            and returns an array of numerical values.
            For example:

            -   ``"sort"`` (string)
            -   :py:func:`sort <openeo.processes.sort>` (:ref:`predefined openEO process function <openeo_processes_functions>`)
            -   ``lambda data: data.concat([42, -3])`` (function or lambda)


        :param version: [**deprecated**] Version of the UDF runtime to use
        :param dimension: The name of the source dimension to apply the process on. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target_dimension: The name of the target dimension or null (the default) to use the source dimension
            specified in the parameter dimension. By specifying a target dimension, the source dimension is removed.
            The target dimension with the specified name and the type other (see add_dimension) is created, if it doesn't exist yet.
        :param context: Additional data to be passed to the process.

        :return: A datacube with the UDF applied to the given dimension.
        :raises: DimensionNotAvailable

        .. versionchanged:: 0.13.0
            arguments ``code``, ``runtime`` and ``version`` are deprecated if favor of the standard approach
            of using an :py:class:`UDF <openeo.rest._datacube.UDF>` object in the ``process`` argument.
            See :ref:`old_udf_api` for more background about the changes.

        .. versionchanged:: 0.42.0
            Not explicitly specifying the ``dimension`` argument is deprecated and will trigger warnings.
            The original fallback value "t" is still in place, but that will be removed in a future version.
        """
        # TODO #137 #181 #312 remove support for code/runtime/version
        if runtime or (isinstance(code, str) and "\n" in code) or version:
            if process:
                raise ValueError(
                    "Cannot specify `process` argument together with deprecated `code`/`runtime`/`version` arguments."
                )
            else:
                warnings.warn(
                    "Specifying UDF code through `code`, `runtime` and `version` arguments is deprecated. "
                    "Instead create an `openeo.UDF` object and pass that to the `process` argument.",
                    category=UserDeprecationWarning,
                    stacklevel=2,
                )
                process = UDF(code=code, runtime=runtime, version=version, context=context)
        else:
            process = process or code
        process = build_child_callback(
            process=process, parent_parameters=["data", "context"], connection=self.connection
        )

        if dimension is _UNSET:
            # TODO #774 fully drop this dimension fallback value after deprecation period
            warnings.warn(
                "Using `DataCube.apply_dimension()` without explicit `dimension` argument is deprecated and will become invalid in a future version. "
                "Falling back on dimension 't'.",
                category=UserDeprecationWarning,
                stacklevel=2,
            )
            dimension = "t"

        arguments = {
            "data": THIS,
            "process": process,
            "dimension": self._assert_valid_dimension_name(dimension),
        }

        metadata = self.metadata
        if target_dimension is not None:
            arguments["target_dimension"] = target_dimension
            metadata = self.metadata.reduce_dimension(dimension_name=dimension) if self.metadata else None
            if(not target_dimension in self.metadata.dimension_names()):
                metadata = self.metadata.add_dimension(target_dimension, label="unknown")
        if context is not None:
            arguments["context"] = context
        result_cube = self.process(process_id="apply_dimension", arguments=arguments, metadata = metadata)

        return result_cube

    @openeo_process
    def reduce_dimension(
        self,
        dimension: str,
        reducer: Union[str, typing.Callable, UDF, PGNode],
        context: Optional[dict] = None,
        process_id="reduce_dimension",
        band_math_mode: bool = False,
    ) -> DataCube:
        """
        Add a reduce process with given reducer callback along given dimension

        :param dimension: the label of the dimension to reduce
        :param reducer: the "child callback":
            the name of a single openEO process,
            or a callback function as discussed in :ref:`callbackfunctions`,
            or a :py:class:`UDF <openeo.rest._datacube.UDF>` instance.

            The callback should correspond to a process that
            receives an array of numerical values
            and returns a single numerical value.
            For example:

            -   ``"mean"`` (string)
            -   :py:func:`absolute <openeo.processes.max>` (:ref:`predefined openEO process function <openeo_processes_functions>`)
            -   ``lambda data: data.min()`` (function or lambda)

        :param context: Additional data to be passed to the process.
        """
        # TODO: check if dimension is valid according to metadata? #116
        # TODO: #125 use/test case for `reduce_dimension_binary`?
        reducer = build_child_callback(
            process=reducer, parent_parameters=["data", "context"], connection=self.connection
        )

        return self.process_with_node(
            ReduceNode(
                process_id=process_id,
                data=self,
                reducer=reducer,
                dimension=self._assert_valid_dimension_name(dimension),
                context=context,
                # TODO #123 is it (still) necessary to make "band" math a special case?
                band_math_mode=band_math_mode,
            ),
            metadata=self.metadata.reduce_dimension(dimension_name=dimension) if self.metadata else None,
        )

    @openeo_process
    def reduce_spatial(
        self,
        reducer: Union[str, typing.Callable, UDF, PGNode],
        context: Optional[dict] = None,
    ) -> "DataCube":
        """
        Add a reduce process with given reducer callback along the spatial dimensions

        :param reducer: the "child callback":
            the name of a single openEO process,
            or a callback function as discussed in :ref:`callbackfunctions`,
            or a :py:class:`UDF <openeo.rest._datacube.UDF>` instance.

            The callback should correspond to a process that
            receives an array of numerical values
            and returns a single numerical value.
            For example:

            -   ``"mean"`` (string)
            -   :py:func:`absolute <openeo.processes.max>` (:ref:`predefined openEO process function <openeo_processes_functions>`)
            -   ``lambda data: data.min()`` (function or lambda)

        :param context: Additional data to be passed to the process.
        """
        reducer = build_child_callback(
            process=reducer, parent_parameters=["data", "context"], connection=self.connection
        )
        return self.process(
            process_id="reduce_spatial",
            data=self,
            reducer=reducer,
            context=context,
            metadata=self.metadata.reduce_spatial(),
        )

    @deprecated("Use :py:meth:`apply_polygon`.", version="0.26.0")
    def chunk_polygon(
        self,
        chunks: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, VectorCube],
        process: Union[str, PGNode, typing.Callable, UDF],
        mask_value: float = None,
        context: Optional[dict] = None,
    ) -> DataCube:
        """"""
        process = build_child_callback(process, parent_parameters=["data"], connection=self.connection)
        valid_geojson_types = [
            "Polygon",
            "MultiPolygon",
            "GeometryCollection",  # TODO #706 stop allowing GeometryCollection
            "Feature",
            "FeatureCollection",
        ]
        chunks = _get_geometry_argument(
            chunks, valid_geojson_types=valid_geojson_types, connection=self.connection
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

    @openeo_process
    def apply_polygon(
        self,
        geometries: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, VectorCube] = None,
        process: Union[str, PGNode, typing.Callable, UDF] = None,
        mask_value: Optional[float] = None,
        context: Optional[dict] = None,
        **kwargs,
    ) -> DataCube:
        """
        Apply a process to segments of the data cube that are defined by the given polygons.
        For each polygon provided, all pixels for which the point at the pixel center intersects
        with the polygon (as defined in the Simple Features standard by the OGC) are collected into sub data cubes.
        If a pixel is part of multiple of the provided polygons (e.g., when the polygons overlap),
        the GeometriesOverlap exception is thrown.
        Each sub data cube is passed individually to the given process.

        :param geometries: Can be provided in different ways:

            - a shapely geometry
            - a GeoJSON-style dictionary,
            - a public URL to the geometries in a vector format that is supported by the backend
              (also see :py:func:`Connection.list_file_formats() <openeo.rest.connection.Connection.list_file_formats>`),
              e.g. GeoJSON, GeoParquet, etc.
              A ``load_url`` process will automatically be added to the process graph.
            - a path (:py:class:`str` or :py:class:`~pathlib.Path`) to a local, client-side GeoJSON file,
              which will be loaded automatically to get the geometries as GeoJSON construct.
            - a :py:class:`~openeo.rest.vectorcube.VectorCube` instance.
            - a :py:class:`~openeo.api.process.Parameter` instance.

        :param process: "child callback" function, see :ref:`callbackfunctions`
        :param mask_value: The value used for pixels outside the polygon.
        :param context: Additional data to be passed to the process.

        .. warning:: experimental process: not generally supported, API subject to change.

        .. versionchanged:: 0.32.0
            Argument ``polygons`` was renamed to ``geometries``.
            While deprecated, the old name ``polygons`` is still supported
            as keyword argument for backwards compatibility.

        .. versionchanged:: 0.36.0
            Support passing a URL as ``geometries`` argument, which will be loaded with the ``load_url`` process.

        .. versionchanged:: 0.36.0
            Support for passing a backend-side path as ``geometries`` argument was removed
            (also see :ref:`legacy_read_vector`).
            Instead, it's possible to provide a client-side path to a GeoJSON file
            (which will be loaded client-side to get the geometries as GeoJSON construct).
        """
        # TODO drop support for legacy `polygons` argument:
        #      remove `kwargs, remove default `None` value for `geometries` and `process`
        #      and the related backwards compatibility code
        geometries_parameter = "geometries"
        if geometries is None and "polygons" in kwargs:
            geometries = kwargs.pop("polygons")
            geometries_parameter = "polygons"
            warnings.warn(
                "In `apply_polygon` use argument `geometries` instead of deprecated 'polygons'.",
                category=UserDeprecationWarning,
                stacklevel=2,
            )
        if kwargs:
            raise ValueError(f"Unexpected keyword arguments: {kwargs!r}")
        if not geometries:
            raise ValueError("No geometries provided.")

        # Note: the `process` argument was given a default value `None` (with the `polygons`/`geometries` argument rename)
        # to keep support for legacy `cube.apply_polygon(polygons=..., process=...)` usage:
        # `geometries` had to be given a default value, and so did `process` as it comes after it.
        # TODO: remove default value for `process` when dropping support for legacy `polygons` argument
        assert process is not None

        process = build_child_callback(process, parent_parameters=["data"], connection=self.connection)
        valid_geojson_types = ["Polygon", "MultiPolygon", "Feature", "FeatureCollection"]
        geometries = _get_geometry_argument(geometries, valid_geojson_types=valid_geojson_types, connection=self.connection)
        mask_value = float(mask_value) if mask_value is not None else None
        return self.process(
            process_id="apply_polygon",
            data=THIS,
            **{geometries_parameter: geometries},
            process=process,
            arguments=dict_no_none(
                mask_value=mask_value,
                context=context,
            ),
        )

    def reduce_bands(self, reducer: Union[str, PGNode, typing.Callable, UDF]) -> DataCube:
        """
        Shortcut for :py:meth:`reduce_dimension` along the band dimension

        :param reducer: "child callback" function, see :ref:`callbackfunctions`
        """
        return self.reduce_dimension(
            dimension=self.metadata.band_dimension.name if self.metadata else "bands",
            reducer=reducer,
            band_math_mode=True,
        )

    def reduce_temporal(self, reducer: Union[str, PGNode, typing.Callable, UDF]) -> DataCube:
        """
        Shortcut for :py:meth:`reduce_dimension` along the temporal dimension

        :param reducer: "child callback" function, see :ref:`callbackfunctions`
        """
        return self.reduce_dimension(
            dimension=self.metadata.temporal_dimension.name if self.metadata else "t",
            reducer=reducer,
        )

    @deprecated(
        "Use :py:meth:`reduce_bands` with :py:class:`UDF <openeo.rest._datacube.UDF>` as reducer.",
        version="0.13.0",
    )
    def reduce_bands_udf(self, code: str, runtime: Optional[str] = None, version: Optional[str] = None) -> DataCube:
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
            metadata=self.metadata.add_dimension(name=name, label=label, type=type) if self.metadata else None,
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
            metadata=self.metadata.drop_dimension(name=name) if self.metadata else None,
        )

    @deprecated(
        "Use :py:meth:`reduce_temporal` with :py:class:`UDF <openeo.rest._datacube.UDF>` as reducer",
        version="0.13.0",
    )
    def reduce_temporal_udf(self, code: str, runtime="Python", version="latest"):
        """
        Apply reduce (`reduce_dimension`) process with given UDF along temporal dimension.

        :param code: The UDF code, compatible with the given runtime and version
        :param runtime: The UDF runtime
        :param version: The UDF runtime version
        """
        # TODO #181 #312 drop this deprecated pattern
        return self.reduce_temporal(reducer=UDF(code=code, runtime=runtime, version=version))

    reduce_tiles_over_time = legacy_alias(
        reduce_temporal_udf, name="reduce_tiles_over_time", since="0.1.1"
    )

    @openeo_process
    def apply_neighborhood(
            self,
            process: Union[str, PGNode, typing.Callable, UDF],
            size: List[Dict],
            overlap: List[dict] = None,
            context: Optional[dict] = None,
    ) -> DataCube:
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
                process=build_child_callback(process=process, parent_parameters=["data"], connection=self.connection),
                size=size,
                overlap=overlap,
                context=context,
            )
        )

    @openeo_process
    def apply(
        self,
        process: Union[str, typing.Callable, UDF, PGNode],
        context: Optional[dict] = None,
    ) -> DataCube:
        """
        Applies a unary process (a local operation) to each value of the specified or all dimensions in the data cube.

        :param process: the "child callback":
            the name of a single process,
            or a callback function as discussed in :ref:`callbackfunctions`,
            or a :py:class:`UDF <openeo.rest._datacube.UDF>` instance.

            The callback should correspond to a process that
            receives a single numerical value
            and returns a single numerical value.
            For example:

            -   ``"absolute"`` (string)
            -   :py:func:`absolute <openeo.processes.absolute>` (:ref:`predefined openEO process function <openeo_processes_functions>`)
            -   ``lambda x: x * 2 + 3`` (function or lambda)

        :param context: Additional data to be passed to the process.

        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        return self.process(
            process_id="apply",
            arguments=dict_no_none(
                {
                    "data": THIS,
                    "process": build_child_callback(process, parent_parameters=["x"], connection=self.connection),
                    "context": context,
                }
            ),
        )

    reduce_temporal_simple = legacy_alias(
        reduce_temporal, "reduce_temporal_simple", since="0.13.0"
    )

    @openeo_process(process_id="min", mode="reduce_dimension")
    def min_time(self) -> DataCube:
        """
        Finds the minimum value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("min")

    @openeo_process(process_id="max", mode="reduce_dimension")
    def max_time(self) -> DataCube:
        """
        Finds the maximum value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("max")

    @openeo_process(process_id="mean", mode="reduce_dimension")
    def mean_time(self) -> DataCube:
        """
        Finds the mean value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("mean")

    @openeo_process(process_id="median", mode="reduce_dimension")
    def median_time(self) -> DataCube:
        """
        Finds the median value of a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("median")

    @openeo_process(process_id="count", mode="reduce_dimension")
    def count_time(self) -> DataCube:
        """
        Counts the number of images with a valid mask in a time series for all bands of the input dataset.

        :return: a DataCube instance
        """
        return self.reduce_temporal("count")

    @openeo_process
    def aggregate_temporal(
        self,
        intervals: List[list],
        reducer: Union[str, typing.Callable, PGNode],
        labels: Optional[List[str]] = None,
        dimension: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> DataCube:
        """
        Computes a temporal aggregation based on an array of date and/or time intervals.

        Calendar hierarchies such as year, month, week etc. must be transformed into specific intervals by the clients. For each interval, all data along the dimension will be passed through the reducer. The computed values will be projected to the labels, so the number of labels and the number of intervals need to be equal.

        If the dimension is not set, the data cube is expected to only have one temporal dimension.

        :param intervals: Temporal left-closed intervals so that the start time is contained, but not the end time.
        :param reducer: the "child callback":
            the name of a single openEO process,
            or a callback function as discussed in :ref:`callbackfunctions`,
            or a :py:class:`UDF <openeo.rest._datacube.UDF>` instance.

            The callback should correspond to a process that
            receives an array of numerical values
            and returns a single numerical value.
            For example:

            -   ``"mean"`` (string)
            -   :py:func:`absolute <openeo.processes.max>` (:ref:`predefined openEO process function <openeo_processes_functions>`)
            -   ``lambda data: data.min()`` (function or lambda)

        :param labels: Labels for the intervals. The number of labels and the number of groups need to be equal.
        :param dimension: The temporal dimension for aggregation. All data along the dimension will be passed through the specified reducer. If the dimension is not set, the data cube is expected to only have one temporal dimension.
        :param context: Additional data to be passed to the reducer. Not set by default.

        :return: A :py:class:`DataCube` containing a result for each time window
        """
        return self.process(
            process_id="aggregate_temporal",
            arguments=dict_no_none(
                data=THIS,
                intervals=intervals,
                labels=labels,
                dimension=dimension,
                reducer=build_child_callback(reducer, parent_parameters=["data"]),
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
    ) -> DataCube:
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
                reducer=build_child_callback(reducer, parent_parameters=["data"]),
                context=context,
            ),
        )

    @openeo_process
    def ndvi(self, nir: str = None, red: str = None, target_band: str = None) -> DataCube:
        """
        Normalized Difference Vegetation Index (NDVI)

        :param nir: (optional) name of NIR band
        :param red: (optional) name of red band
        :param target_band: (optional) name of the newly created band

        :return: a DataCube instance
        """
        if self.metadata is None:
            metadata = None
        elif target_band is None:
            metadata = self.metadata.reduce_dimension(self.metadata.band_dimension.name)
        else:
            # TODO: first drop "bands" dim and re-add it with single "ndvi" band
            metadata = self.metadata.append_band(Band(name=target_band, common_name="ndvi"))
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
        if self._do_metadata_normalization() and target in self.metadata.dimension_names():
            raise ValueError('Target dimension name conflicts with existing dimension: %s.' % target)
        return self.process(
            process_id="rename_dimension",
            arguments=dict_no_none(
                data=THIS,
                source=self._assert_valid_dimension_name(source),
                target=target,
            ),
            metadata=self.metadata.rename_dimension(source, target) if self.metadata else None,
        )

    @openeo_process
    def rename_labels(self, dimension: str, target: list, source: list = None) -> DataCube:
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
                dimension=self._assert_valid_dimension_name(dimension),
                target=target,
                source=source,
            ),
            metadata=self.metadata.rename_labels(dimension, target, source) if self.metadata else None,
        )

    @openeo_process(mode="apply")
    def linear_scale_range(self, input_min, input_max, output_min, output_max) -> DataCube:
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
    def mask(self, mask: DataCube = None, replacement=None) -> DataCube:
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
        mask: Union[shapely.geometry.base.BaseGeometry, dict, str, pathlib.Path, Parameter, VectorCube],
        srs: str = None,
        replacement=None,
        inside: bool = None,
    ) -> DataCube:
        """
        Applies a polygon mask to a raster data cube. To apply a raster mask use `mask`.

        All pixels for which the point at the pixel center does not intersect with any
        polygon (as defined in the Simple Features standard by the OGC) are replaced.
        This behaviour can be inverted by setting the parameter `inside` to true.

        The pixel values are replaced with the value specified for `replacement`,
        which defaults to `no data`.

        :param mask: The geometry to mask with.an be provided in different ways:

            - a shapely geometry
            - a GeoJSON-style dictionary,
            - a public URL to the geometries in a vector format that is supported by the backend
              (also see :py:func:`Connection.list_file_formats() <openeo.rest.connection.Connection.list_file_formats>`),
              e.g. GeoJSON, GeoParquet, etc.
              A ``load_url`` process will automatically be added to the process graph.
            - a path (:py:class:`str` or :py:class:`~pathlib.Path`) to a local, client-side GeoJSON file,
              which will be loaded automatically to get the geometries as GeoJSON construct.
            - a :py:class:`~openeo.rest.vectorcube.VectorCube` instance.
            - a :py:class:`~openeo.api.process.Parameter` instance.

        :param srs: The spatial reference system of the provided polygon.
            By default longitude-latitude (EPSG:4326) is assumed.

            .. note:: this ``srs`` argument is a non-standard/experimental feature, only supported by specific back-ends.
                See https://github.com/Open-EO/openeo-processes/issues/235 for details.
        :param replacement: the value to replace the masked pixels with

        .. versionchanged:: 0.36.0
            Support passing a URL as ``geometries`` argument, which will be loaded with the ``load_url`` process.

        .. versionchanged:: 0.36.0
            Support for passing a backend-side path as ``geometries`` argument was removed
            (also see :ref:`legacy_read_vector`).
            Instead, it's possible to provide a client-side path to a GeoJSON file
            (which will be loaded client-side to get the geometries as GeoJSON construct).
        """
        valid_geojson_types = [
            "Polygon",
            "MultiPolygon",
            "GeometryCollection",  # TODO #706 stop allowing GeometryCollection
            "Feature",
            "FeatureCollection",
        ]
        mask = _get_geometry_argument(
            mask, valid_geojson_types=valid_geojson_types, connection=self.connection, crs=srs
        )
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
        other: DataCube,
        overlap_resolver: Union[str, PGNode, typing.Callable] = None,
        context: Optional[dict] = None,
    ) -> DataCube:
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
            arguments["overlap_resolver"] = build_child_callback(overlap_resolver, parent_parameters=["x", "y"])
        if (
            self.metadata
            and self.metadata.has_band_dimension()
            and isinstance(other, DataCube)
            and other.metadata
            and other.metadata.has_band_dimension()
        ):
            # Minimal client side metadata merging
            merged_metadata = self.metadata
            for b in other.metadata.band_dimension.bands:
                if b not in merged_metadata.bands:
                    merged_metadata = merged_metadata.append_band(b)
        else:
            merged_metadata = None
        # Overlapping bands without overlap resolver will give an error in the backend
        if context:
            arguments["context"] = context
        return self.process(process_id="merge_cubes", arguments=arguments, metadata=merged_metadata)

    merge = legacy_alias(merge_cubes, name="merge", since="0.4.6")

    @openeo_process
    def apply_kernel(
            self, kernel: Union[np.ndarray, List[List[float]]], factor=1.0, border=0,
            replace_invalid=0
    ) -> DataCube:
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
    ) -> DataCube:
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
        return VectorCube(pg_node, connection=self._connection)

    ####VIEW methods #######

    @deprecated(
        "Use :py:meth:`aggregate_spatial` with reducer ``'mean'``.", version="0.10.0"
    )
    def polygonal_mean_timeseries(
        self, polygon: Union[Polygon, MultiPolygon, str]
    ) -> VectorCube:
        """
        Extract a mean time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        """
        return self.aggregate_spatial(geometries=polygon, reducer="mean")

    @deprecated(
        "Use :py:meth:`aggregate_spatial` with reducer ``'histogram'``.",
        version="0.10.0",
    )
    def polygonal_histogram_timeseries(
        self, polygon: Union[Polygon, MultiPolygon, str]
    ) -> VectorCube:
        """
        Extract a histogram time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        """
        return self.aggregate_spatial(geometries=polygon, reducer="histogram")

    @deprecated(
        "Use :py:meth:`aggregate_spatial` with reducer ``'median'``.", version="0.10.0"
    )
    def polygonal_median_timeseries(
        self, polygon: Union[Polygon, MultiPolygon, str]
    ) -> VectorCube:
        """
        Extract a median time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        """
        return self.aggregate_spatial(geometries=polygon, reducer="median")

    @deprecated(
        "Use :py:meth:`aggregate_spatial` with reducer ``'sd'``.", version="0.10.0"
    )
    def polygonal_standarddeviation_timeseries(
        self, polygon: Union[Polygon, MultiPolygon, str]
    ) -> VectorCube:
        """
        Extract a time series of standard deviations for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        """
        return self.aggregate_spatial(geometries=polygon, reducer="sd")

    @openeo_process
    def ard_surface_reflectance(
            self, atmospheric_correction_method: str, cloud_detection_method: str, elevation_model: str = None,
            atmospheric_correction_options: dict = None, cloud_detection_options: dict = None,
    ) -> DataCube:
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
    def atmospheric_correction(self, method: str = None, elevation_model: str = None, options: dict = None) -> DataCube:
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
    def save_result(
        self,
        # TODO: does it make sense for the client to define a (hard coded) default format here?
        format: str = _DEFAULT_RASTER_FORMAT,
        options: Optional[dict] = None,
    ) -> SaveResult:
        """
        Materialize the processed data to the given file format.

        :param format: an output format supported by the backend.
        :param options: (optional) file format options

        .. versionchanged:: 0.39.0
            returns a :py:class:`~openeo.rest.result.SaveResult` instance instead
            of another :py:class:`~openeo.rest.datacube.DataCube` instance.
        """
        if self._connection:
            formats = set(self._connection.list_output_formats().keys())
            # TODO: map format to correct casing too?
            if format.lower() not in {f.lower() for f in formats}:
                raise ValueError("Invalid format {f!r}. Should be one of {s}".format(f=format, s=formats))

        pg = self._build_pgnode(
            process_id="save_result",
            arguments={
                "data": self,
                "format": format,
                # TODO: leave out options if unset?
                "options": options or {},
            },
        )
        return SaveResult(pg, connection=self._connection)

    def _auto_save_result(
        self,
        format: Optional[str] = None,
        outputfile: Optional[Union[str, pathlib.Path]] = None,
        options: Optional[dict] = None,
    ) -> SaveResult:
        if any(n.process_id == "save_result" for n in self.result_node().walk_nodes()):
            log.warning(f"This {type(self).__name__} already contains 1 or more `save_result` nodes.")

        return self.save_result(
            format=format or (guess_format(outputfile) if outputfile else None) or self._DEFAULT_RASTER_FORMAT,
            options=options,
        )

    def download(
        self,
        outputfile: Optional[Union[str, pathlib.Path]] = None,
        format: Optional[str] = None,
        options: Optional[dict] = None,
        *,
        validate: Optional[bool] = None,
        auto_add_save_result: bool = True,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        on_response_headers: Optional[Callable[[Mapping], None]] = None,
    ) -> Union[None, bytes]:
        """
        Send the underlying process graph to the backend
        for synchronous processing and directly download the result.

        If ``outputfile`` is provided, the result is downloaded to that path.
        Otherwise a :py:class:`bytes` object is returned with the raw data.

        :param outputfile: (optional) output path to download to.
        :param format: (optional) an output format supported by the backend.
        :param options: (optional) file format options
        :param validate: (optional) toggle to enable/prevent validation of the process graphs before execution
            (overruling the connection's ``auto_validate`` setting).
        :param auto_add_save_result: whether to automatically add a ``save_result`` node to the process graph.
        :param additional: (optional) additional (top-level) properties to set in the request body
        :param job_options: (optional) dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param on_response_headers: (optional) callback to handle/show the response headers

        :return: if ``outputfile`` was not specified:
            a :py:class:`bytes` object containing the raw data.
            Otherwise, ``None`` is returned.

        .. versionchanged:: 0.32.0
            Added ``auto_add_save_result`` option

        .. versionchanged:: 0.36.0
            Added arguments ``additional`` and ``job_options``.

        .. versionchanged:: 0.40
            Added argument ``on_response_headers``.
        """
        # TODO #278 centralize download/create_job/execute_job logic in DataCube, VectorCube, MlModel, ...
        if auto_add_save_result:
            res = self._auto_save_result(format=format, outputfile=outputfile, options=options)
        else:
            res = self
        return self._connection.download(
            res.flat_graph(),
            outputfile=outputfile,
            validate=validate,
            additional=additional,
            job_options=job_options,
            on_response_headers=on_response_headers,
        )

    def validate(self) -> ValidationResponse:
        """
        Validate a process graph without executing it.

        :return: container of validation of errors (dictionaries with "code" and "message" fields)

        .. versionchanged:: 0.38.0
            returns a :py:class:`~openeo.rest.models.general.ValidationResponse` object
            instead of a simple list of error dictionaries.
        """
        # TODO this method implementation does not really override something
        #       it is just kept to override the doc.
        #       At some point this should be removed for simplicity.
        return super().validate()

    def tiled_viewing_service(self, type: str, **kwargs) -> Service:
        return self._connection.create_service(self.flat_graph(), type=type, **kwargs)

    def _get_spatial_extent_from_load_collection(self):
        pg = self.flat_graph()
        for node in pg:
            if pg[node]["process_id"] == "load_collection":
                if "spatial_extent" in pg[node]["arguments"] and all(
                    cd in pg[node]["arguments"]["spatial_extent"] for cd in ["east", "west", "south", "north"]
                ):
                    return pg[node]["arguments"]["spatial_extent"]
        return None

    def preview(
        self,
        center: Union[Iterable, None] = None,
        zoom: Union[int, None] = None,
    ):
        """
        Creates a service with the process graph and displays a map widget. Only supports XYZ.

        :param center: (optional) Map center. Default is (0,0).
        :param zoom: (optional) Zoom level of the map. Default is 1.

        :return: ipyleaflet Map object and the displayed Service

        .. warning:: experimental feature, subject to change.
        .. versionadded:: 0.19.0
        """
        if "XYZ" not in self.connection.list_service_types():
            raise OpenEoClientException("Backend does not support service type 'XYZ'.")

        if not in_jupyter_context():
            raise Exception("On-demand preview only supported in Jupyter notebooks!")
        try:
            import ipyleaflet
        except ImportError:
            raise Exception(
                "Additional modules must be installed for on-demand preview. Run `pip install openeo[jupyter]` or refer to the documentation."
            )

        service = self.tiled_viewing_service("XYZ")
        service_metadata = service.describe_service()

        m = ipyleaflet.Map(
            center=center or (0, 0),
            zoom=zoom or 1,
            scroll_wheel_zoom=True,
            basemap=ipyleaflet.basemaps.OpenStreetMap.Mapnik,
        )
        service_layer = ipyleaflet.TileLayer(url=service_metadata["url"])
        m.add(service_layer)

        if center is None and zoom is None:
            spatial_extent = self._get_spatial_extent_from_load_collection()
            if spatial_extent is not None:
                m.fit_bounds(
                    [
                        [spatial_extent["south"], spatial_extent["west"]],
                        [spatial_extent["north"], spatial_extent["east"]],
                    ]
                )

        class Preview:
            """
            On-demand preview instance holding the associated XYZ service and ipyleaflet Map
            """

            def __init__(self, service: Service, ipyleaflet_map: ipyleaflet.Map):
                self.service = service
                self.map = ipyleaflet_map

            def _repr_html_(self):
                from IPython.display import display

                display(self.map)

            def delete_service(self):
                self.service.delete_service()

        return Preview(service, m)

    def execute_batch(
        self,
        outputfile: Optional[Union[str, pathlib.Path]] = None,
        out_format: Optional[str] = None,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        plan: Optional[str] = None,
        budget: Optional[float] = None,
        print: typing.Callable[[str], None] = print,
        max_poll_interval: float = DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
        connection_retry_interval: float = DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
        auto_add_save_result: bool = True,
        show_error_logs: bool = True,
        log_level: Optional[str] = None,
        # TODO: deprecate `format_options` as keyword arguments
        **format_options,
    ) -> BatchJob:
        """
        Execute the underlying process graph at the backend in batch job mode:

        - create the job (like :py:meth:`create_job`)
        - start the job (like :py:meth:`BatchJob.start() <openeo.rest.job.BatchJob.start>`)
        - track the job's progress with an active polling loop
          (like :py:meth:`BatchJob.run_synchronous() <openeo.rest.job.BatchJob.run_synchronous>`)
        - optionally (if ``outputfile`` is specified) download the job's results
          when the job finished successfully

        .. note::
            Because of the active polling loop,
            which blocks any further progress of your script or application,
            this :py:meth:`execute_batch` method is mainly recommended
            for batch jobs that are expected to complete
            in a time that is reasonable for your use case.

        :param outputfile: (optional) output path to download to.
        :param out_format: (optional) file format to use for the job result.
        :param title: (optional) job title.
        :param description: (optional) job description.
        :param plan: (optional) the billing plan to process and charge the job with.
        :param budget: (optional) maximum budget to be spent on executing the job.
            Note that some backends do not honor this limit.
        :param additional: (optional) additional (top-level) properties to set in the request body
        :param job_options: (optional) dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param validate: (optional) toggle to enable/prevent validation of the process graphs before execution
            (overruling the connection's ``auto_validate`` setting).
        :param auto_add_save_result: whether to automatically add a ``save_result`` node to the process graph.
        :param show_error_logs: whether to automatically print error logs when the batch job failed.
        :param log_level: (optional) minimum severity level for log entries that the back-end should keep track of.
            One of "error" (highest severity), "warning", "info", and "debug" (lowest severity).
        :param max_poll_interval: maximum number of seconds to sleep between job status polls
        :param connection_retry_interval: how long to wait when status poll failed due to connection issue
        :param print: print/logging function to show progress/status

        :return: Handle to the job created at the backend.

        .. versionchanged:: 0.32.0
            Added ``auto_add_save_result`` option

        .. versionchanged:: 0.36.0
            Added argument ``additional``.

        .. versionchanged:: 0.37.0
            Added argument ``show_error_logs``.

        .. versionchanged:: 0.37.0
            Added argument ``log_level``.
        """
        # TODO: start showing deprecation warnings about these inconsistent argument names
        if "format" in format_options and not out_format:
            out_format = format_options["format"]  # align with 'download' call arg name

        # TODO #278 centralize download/create_job/execute_job logic in DataCube, VectorCube, MlModel, ...
        if auto_add_save_result:
            res = self._auto_save_result(format=out_format, outputfile=outputfile, options=format_options)
            create_kwargs = {}
        else:
            res = self
            create_kwargs = {"auto_add_save_result": False}

        job = res.create_job(
            title=title,
            description=description,
            plan=plan,
            budget=budget,
            additional=additional,
            job_options=job_options,
            validate=validate,
            log_level=log_level,
            **create_kwargs,
        )
        job.start_and_wait(
            print=print,
            max_poll_interval=max_poll_interval,
            connection_retry_interval=connection_retry_interval,
            show_error_logs=show_error_logs,
        )
        if outputfile is not None:
            job.download_result(target=outputfile)
        return job

    def create_job(
        self,
        out_format: Optional[str] = None,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        plan: Optional[str] = None,
        budget: Optional[float] = None,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
        auto_add_save_result: bool = True,
        log_level: Optional[str] = None,
        # TODO: avoid `format_options` as keyword arguments
        **format_options,
    ) -> BatchJob:
        """
        Send the underlying process graph to the backend
        to create an openEO batch job
        and return a corresponding :py:class:`~openeo.rest.job.BatchJob` instance.

        Note that this method only *creates* the openEO batch job at the backend,
        but it does not *start* it.
        Use :py:meth:`execute_batch` instead to let the openEO Python client
        take care of the full job life cycle: create, start and track its progress until completion.

        :param out_format: (optional) file format to use for the job result.
        :param title: (optional) job title.
        :param description: (optional) job description.
        :param plan: (optional) the billing plan to process and charge the job with.
        :param budget: (optional) maximum budget to be spent on executing the job.
            Note that some backends do not honor this limit.
        :param additional: (optional) additional (top-level) properties to set in the request body
        :param job_options: (optional) dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param validate: (optional) toggle to enable/prevent validation of the process graphs before execution
            (overruling the connection's ``auto_validate`` setting).
        :param auto_add_save_result: whether to automatically add a ``save_result`` node to the process graph.
        :param log_level: (optional) minimum severity level for log entries that the back-end should keep track of.
            One of "error" (highest severity), "warning", "info", and "debug" (lowest severity).

        :return: Handle to the job created at the backend.

        .. versionchanged:: 0.32.0
            Added ``auto_add_save_result`` option

        .. versionchanged:: 0.36.0
            Added ``additional`` argument.

        .. versionchanged:: 0.37.0
            Added argument ``log_level``.
        """
        # TODO: add option to also automatically start the job?
        # TODO: avoid using all kwargs as format_options
        # TODO #278 centralize download/create_job/execute_job logic in DataCube, VectorCube, MlModel, ...
        if auto_add_save_result:
            res = self._auto_save_result(format=out_format, options=format_options)
        else:
            res = self

        return self._connection.create_job(
            process_graph=res.flat_graph(),
            title=title,
            description=description,
            plan=plan,
            budget=budget,
            validate=validate,
            additional=additional,
            job_options=job_options,
            log_level=log_level,
        )

    send_job = legacy_alias(create_job, name="send_job", since="0.10.0")

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

    def execute(self, *, validate: Optional[bool] = None, auto_decode: bool = True) -> Union[dict, requests.Response]:
        """
        Execute a process graph synchronously and return the result. If the result is a JSON object, it will be parsed.

        :param validate: Optional toggle to enable/prevent validation of the process graphs before execution
            (overruling the connection's ``auto_validate`` setting).
        :param auto_decode: Boolean flag to enable/disable automatic JSON decoding of the response. Defaults to True.

        :return: parsed JSON response as a dict if auto_decode is True, otherwise response object
        """
        # TODO: deprecated this. It's ill-defined how to "execute" a data cube without downloading it.
        return self._connection.execute(self.flat_graph(), validate=validate, auto_decode=auto_decode)

    @staticmethod
    @deprecated(reason="Use :py:func:`openeo.udf.run_code.execute_local_udf` instead", version="0.7.0")
    def execute_local_udf(udf: str, datacube: Union[str, 'xarray.DataArray', 'XarrayDataCube'] = None, fmt='netcdf'):
        import openeo.udf.run_code
        return openeo.udf.run_code.execute_local_udf(udf=udf, datacube=datacube, fmt=fmt)

    @openeo_process
    def ard_normalized_radar_backscatter(
            self, elevation_model: str = None, contributing_area=False,
            ellipsoid_incidence_angle: bool = False, noise_removal: bool = True
    ) -> DataCube:
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
        coefficient: Union[str, None] = _UNSET,
        elevation_model: Union[str, None] = None,
        mask: bool = False,
        contributing_area: bool = False,
        local_incidence_angle: bool = False,
        ellipsoid_incidence_angle: bool = False,
        noise_removal: bool = True,
        options: Optional[dict] = None,
    ) -> DataCube:
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
        try:
            parameter = Process.from_dict(self.connection.describe_process("sar_backscatter")).get_parameter(
                "coefficient"
            )
            schema = parameter.schema
            coefficient_options = schema.get_enum_options()
            if coefficient == _UNSET:
                coefficient = parameter.default
        except Exception as e:
            log.warning(f"Failed to extract coefficient options for process `sar_backscatter`: {e}")
            coefficient_options = [
                "beta0",
                "sigma0-ellipsoid",
                "sigma0-terrain",
                "gamma0-ellipsoid",
                "gamma0-terrain",
                None,
            ]
            if coefficient == _UNSET:
                coefficient = "gamma0-terrain"
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
        # TODO: does this return a `DataCube`? Shouldn't it just return an array (wrapper)?
        return self.process(
            process_id="fit_curve",
            arguments={
                "data": THIS,
                "parameters": parameters,
                "function": build_child_callback(function, parent_parameters=["x", "parameters"]),
                "dimension": dimension,
            },
        )

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
        return self.process(
            process_id="predict_curve",
            arguments={
                "data": THIS,
                "parameters": parameters,
                "function": build_child_callback(function, parent_parameters=["x", "parameters"]),
                "dimension": dimension,
                "labels": labels,
            },
        )

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
        reducer = PGNode(
            process_id="predict_random_forest", data={"from_parameter": "data"}, model={"from_parameter": "context"}
        )
        return self.reduce_dimension(dimension=dimension, reducer=reducer, context=model)

    @openeo_process
    def dimension_labels(self, dimension: str) -> DataCube:
        """
        Gives all labels for a dimension in the data cube. The labels have the same order as in the data cube.

        :param dimension: The name of the dimension to get the labels for.
        """
        if self._do_metadata_normalization():
            dimension_names = self.metadata.dimension_names()
            if dimension_names and dimension not in dimension_names:
                raise ValueError(f"Invalid dimension name {dimension!r}, should be one of {dimension_names}")
        return self.process(process_id="dimension_labels", arguments={"data": THIS, "dimension": dimension})

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


def _get_geometry_argument(
    argument: Union[
        shapely.geometry.base.BaseGeometry,
        dict,
        str,
        pathlib.Path,
        Parameter,
        _FromNodeMixin,
    ],
    *,
    valid_geojson_types: List[str],
    connection: Connection = None,
    crs: Optional[str] = None,
    allow_parameter: bool = True,
    allow_bounding_box: bool = False,
    allow_none: bool = False,
    argument_name: str = "n/a",
    process_id: str = "n/a",
) -> Union[dict, Parameter, PGNode, _FromNodeMixin, None]:
    """
    Normalize a user input to a openEO-compatible geometry representation,
    like a GeoJSON construct, vector cube reference, bounding box construct,
    a parameter reference, ...

    :param crs: value that encodes a coordinate reference system.
        See :py:func:`openeo.util.normalize_crs` for more details about additional normalization that is applied to this argument.
    :param allow_parameter: allow argument to be a :py:class:`Parameter` instance, and pass-through as such
    :param allow_none: allow argument to be ``None`` and pass-through as such
    :param allow_bounding_box: allow argument to be a bounding box dictionary and pass-through as such
    """
    # Some quick exit shortcuts
    if allow_parameter and isinstance(argument, Parameter):
        if not schema_supports(argument.schema, type="object"):
            warnings.warn(
                f"Schema mismatch with parameter given to `{argument_name}` in `{process_id}`:"
                f" expected a schema compatible with type 'object' but got {argument.schema!r}."
            )
        return argument
    elif isinstance(argument, _FromNodeMixin):
        # Typical use case here: VectorCube instance
        return argument.from_node()
    elif allow_none and argument is None:
        return argument
    elif (
        allow_bounding_box
        and isinstance(argument, dict)
        and all(k in argument for k in ["west", "south", "east", "north"])
    ):
        return argument

    # Support URL based geometry references (with `load_url` and best-effort format guess)
    if isinstance(argument, str) and re.match(r"^https?://", argument, flags=re.I):
        url = urllib.parse.urlparse(argument)
        suffix = pathlib.Path(url.path.lower()).suffix
        format = {
            ".json": "GeoJSON",
            ".geojson": "GeoJSON",
            ".pq": "Parquet",
            ".parquet": "Parquet",
            ".geoparquet": "Parquet",
        }.get(suffix, suffix.split(".")[-1])
        return connection.load_url(url=argument, format=format)

    # Support loading GeoJSON from local files
    if (
        isinstance(argument, (str, pathlib.Path))
        and pathlib.Path(argument).is_file()
        and pathlib.Path(argument).suffix.lower() in [".json", ".geojson"]
    ):
        geometry = load_json(argument)
    elif isinstance(argument, shapely.geometry.base.BaseGeometry):
        geometry = mapping(argument)
    elif isinstance(argument, dict):
        geometry = argument
    else:
        raise OpenEoClientException(f"Invalid geometry argument: {argument!r}")

    # The assumption at this point is that we are working with a GeoJSON style dictionary
    assert isinstance(geometry, dict)
    if geometry.get("type") not in valid_geojson_types:
        raise OpenEoClientException("Invalid geometry type {t!r}, must be one of {s}".format(
            t=geometry.get("type"), s=valid_geojson_types
        ))
    if geometry.get("type") == "GeometryCollection":
        # TODO #706 Fully drop GeometryCollection support
        warnings.warn(
            "Usage of GeoJSON type 'GeometryCollection' is deprecated/invalid. Support in openEO Python client will be dropped. Use a FeatureCollection instead.",
            category=DeprecationWarning,
        )
    if crs:
        # TODO: don't warn when the crs is Lon-Lat like EPSG:4326?
        warnings.warn(f"Geometry with non-Lon-Lat CRS {crs!r} is only supported by specific back-ends.")
        # TODO #204 alternative for non-standard CRS in GeoJSON object?
        epsg_code = normalize_crs(crs)
        if epsg_code is not None:
            # proj did recognize the CRS
            crs_name = f"EPSG:{epsg_code}"
        else:
            # proj did not recognise this CRS
            warnings.warn(f"non-Lon-Lat CRS {crs!r} is not known to the proj library and might not be supported.")
            crs_name = crs
        geometry["crs"] = {"type": "name", "properties": {"name": crs_name}}
    return geometry

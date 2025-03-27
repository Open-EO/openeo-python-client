from __future__ import annotations

import json
import logging
import pathlib
import typing
from typing import Callable, List, Mapping, Optional, Tuple, Union

import shapely.geometry.base

import openeo.rest.datacube
from openeo.api.process import Parameter
from openeo.internal.documentation import openeo_process
from openeo.internal.graph_building import PGNode
from openeo.internal.warnings import legacy_alias
from openeo.metadata import CollectionMetadata, CubeMetadata, Dimension
from openeo.rest import (
    DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
    DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
)
from openeo.rest._datacube import (
    THIS,
    UDF,
    _ProcessGraphAbstraction,
    build_child_callback,
)
from openeo.rest.job import BatchJob
from openeo.rest.mlmodel import MlModel
from openeo.rest.result import SaveResult
from openeo.util import InvalidBBoxException, dict_no_none, guess_format, to_bbox_dict

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo import Connection


_log = logging.getLogger(__name__)


class VectorCube(_ProcessGraphAbstraction):
    """
    A Vector Cube, or 'Vector Collection' is a data structure containing 'Features':
    https://www.w3.org/TR/sdw-bp/#dfn-feature

    The features in this cube are restricted to have a geometry. Geometries can be points, lines, polygons etcetera.
    A geometry is specified in a 'coordinate reference system'. https://www.w3.org/TR/sdw-bp/#dfn-coordinate-reference-system-(crs)
    """

    _DEFAULT_VECTOR_FORMAT = "GeoJSON"

    def __init__(self, graph: PGNode, connection: Union[Connection, None], metadata: Optional[CubeMetadata] = None):
        super().__init__(pgnode=graph, connection=connection)
        self.metadata = metadata

    @classmethod
    def _build_metadata(cls, add_properties: bool = False) -> CollectionMetadata:
        """Helper to build a (minimal) `CollectionMetadata` object."""
        # Vector cubes have at least a "geometry" dimension
        dimensions = [Dimension(name="geometry", type="geometry")]
        if add_properties:
            dimensions.append(Dimension(name="properties", type="other"))
        # TODO #464: use a more generic metadata container than "collection" metadata
        return CollectionMetadata(metadata={}, dimensions=dimensions)

    def process(
        self,
        process_id: str,
        arguments: dict = None,
        metadata: Optional[CollectionMetadata] = None,
        namespace: Optional[str] = None,
        **kwargs,
    ) -> VectorCube:
        """
        Generic helper to create a new VectorCube by applying a process.

        :param process_id: process id of the process.
        :param args: argument dictionary for the process.
        :return: new VectorCube instance
        """
        pg = self._build_pgnode(process_id=process_id, arguments=arguments, namespace=namespace, **kwargs)
        return VectorCube(graph=pg, connection=self._connection, metadata=metadata or self.metadata)

    @classmethod
    @openeo_process
    def load_geojson(
        cls,
        connection: Connection,
        data: Union[dict, str, pathlib.Path, shapely.geometry.base.BaseGeometry, Parameter],
        properties: Optional[List[str]] = None,
    ) -> VectorCube:
        """
        Converts GeoJSON data as defined by RFC 7946 into a vector data cube.

        :param connection: the connection to use to connect with the openEO back-end.
        :param data: the geometry to load. One of:

            - GeoJSON-style data structure: e.g. a dictionary with ``"type": "Polygon"`` and ``"coordinates"`` fields
            - a path to a local GeoJSON file
            - a GeoJSON string
            - a shapely geometry object

        :param properties: A list of properties from the GeoJSON file to construct an additional dimension from.
        :return: new VectorCube instance

        .. warning:: EXPERIMENTAL: this process is experimental with the potential for major things to change.

        .. versionadded:: 0.22.0
        """
        # TODO: unify with `DataCube._get_geometry_argument`
        # TODO #457 also support client side fetching of GeoJSON from URL?
        if isinstance(data, str) and data.strip().startswith("{"):
            # Assume JSON dump
            geometry = json.loads(data)
        elif isinstance(data, (str, pathlib.Path)):
            # Assume local file
            with pathlib.Path(data).open(mode="r", encoding="utf-8") as f:
                geometry = json.load(f)
                assert isinstance(geometry, dict)
        elif isinstance(data, shapely.geometry.base.BaseGeometry):
            geometry = shapely.geometry.mapping(data)
        elif isinstance(data, Parameter):
            geometry = data
        elif isinstance(data, dict):
            geometry = data
        else:
            raise ValueError(data)
        # TODO #457 client side verification of GeoJSON construct: valid type, valid structure, presence of CRS, ...?

        pg = PGNode(process_id="load_geojson", data=geometry, properties=properties or [])
        # TODO #457 always a "properties" dimension? https://github.com/Open-EO/openeo-processes/issues/448
        metadata = cls._build_metadata(add_properties=True)
        return cls(graph=pg, connection=connection, metadata=metadata)

    @classmethod
    @openeo_process
    def load_url(cls, connection: Connection, url: str, format: str, options: Optional[dict] = None) -> VectorCube:
        """
        Loads a file from a URL

        :param connection: the connection to use to connect with the openEO back-end.
        :param url: The URL to read from. Authentication details such as API keys or tokens may need to be included in the URL.
        :param format: The file format to use when loading the data.
        :param options: The file format parameters to use when reading the data.
            Must correspond to the parameters that the server reports as supported parameters for the chosen ``format``
        :return: new VectorCube instance

        .. warning:: EXPERIMENTAL: this process is experimental with the potential for major things to change.

        .. versionadded:: 0.22.0
        """
        pg = PGNode(process_id="load_url", arguments=dict_no_none(url=url, format=format, options=options))
        # TODO #457 always a "properties" dimension? https://github.com/Open-EO/openeo-processes/issues/448
        metadata = cls._build_metadata(add_properties=True)
        return cls(graph=pg, connection=connection, metadata=metadata)

    @openeo_process
    def run_udf(
        self,
        udf: Union[str, UDF],
        runtime: Optional[str] = None,
        version: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> VectorCube:
        """
        Run a UDF on the vector cube.

        It is recommended to provide the UDF just as :py:class:`UDF <openeo.rest._datacube.UDF>` instance.
        (the other arguments could be used to override UDF parameters if necessary).

        :param udf: UDF code as a string or :py:class:`UDF <openeo.rest._datacube.UDF>` instance
        :param runtime: UDF runtime
        :param version: UDF version
        :param context: UDF context

        .. warning:: EXPERIMENTAL: not generally supported, API subject to change.

        .. versionadded:: 0.10.0

        .. versionchanged:: 0.16.0
            Added support to pass self-contained :py:class:`UDF <openeo.rest._datacube.UDF>` instance.
        """
        if isinstance(udf, UDF):
            # `UDF` instance is preferred usage pattern, but allow overriding.
            version = version or udf.version
            context = context or udf.context
            runtime = runtime or udf.get_runtime(connection=self.connection)
            udf = udf.code
        else:
            if not runtime:
                raise ValueError("Argument `runtime` must be specified")
        return self.process(
            process_id="run_udf",
            data=self, udf=udf, runtime=runtime,
            arguments=dict_no_none({"version": version, "context": context}),
        )

    @openeo_process
    def save_result(
        self,
        # TODO: does it make sense for the client to define a (hard coded) default format here?
        format: Union[str, None] = "GeoJSON",
        options: dict = None,
    ) -> SaveResult:
        """
        Materialize the processed data to the given file format.

        :param format: an output format supported by the backend.
        :param options: (optional) file format options

        .. versionchanged:: 0.39.0
            returns a :py:class:`~openeo.rest.result.SaveResult` instance instead
            of another :py:class:`~openeo.rest.vectorcube.VectorCube` instance.
        """
        pg = self._build_pgnode(
            process_id="save_result",
            arguments={
                "data": self,
                "format": format or "GeoJSON",
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
            _log.warning(f"This {type(self).__name__} already contains 1 or more `save_result` nodes.")

        return self.save_result(
            format=format or (guess_format(outputfile) if outputfile else None) or self._DEFAULT_VECTOR_FORMAT,
            options=options,
        )

    def execute(self, *, validate: Optional[bool] = None) -> dict:
        """Executes the process graph."""
        return self._connection.execute(self.flat_graph(), validate=validate)

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

        .. versionchanged:: 0.21.0
            When not specified explicitly, output format is guessed from output file extension.

        .. versionchanged:: 0.32.0
            Added ``auto_add_save_result`` option

        .. versionchanged:: 0.39.0
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
            res.flat_graph(), outputfile=outputfile, validate=validate, additional=additional, job_options=job_options
        )

    def execute_batch(
        self,
        outputfile: Optional[Union[str, pathlib.Path]] = None,
        out_format: Optional[str] = None,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        plan: Optional[str] = None,
        budget: Optional[float] = None,
        print=print,
        max_poll_interval: float = DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
        connection_retry_interval: float = DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
        auto_add_save_result: bool = True,
        show_error_logs: bool = True,
        log_level: Optional[str] = None,
        # TODO: avoid using kwargs as format options
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

        .. versionchanged:: 0.21.0
            When not specified explicitly, output format is guessed from output file extension.

        .. versionchanged:: 0.32.0
            Added ``auto_add_save_result`` option

        .. versionchanged:: 0.36.0
            Added argument ``additional``.

        .. versionchanged:: 0.37.0
            Added argument ``show_error_logs``.

        .. versionchanged:: 0.37.0
            Added argument ``log_level``.
        """
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

        .. versionchanged:: 0.37.0
            Added argument ``log_level``.
        """
        # TODO: avoid using all kwargs as format_options
        # TODO #278 centralize download/create_job/execute_job logic in DataCube, VectorCube, MlModel, ...
        if auto_add_save_result:
            res = self._auto_save_result(format=out_format, options=format_options or None)
        else:
            res = self
        return self._connection.create_job(
            process_graph=res.flat_graph(),
            title=title,
            description=description,
            plan=plan,
            budget=budget,
            additional=additional,
            job_options=job_options,
            validate=validate,
            log_level=log_level,
        )

    send_job = legacy_alias(create_job, name="send_job", since="0.10.0")

    @openeo_process
    def filter_bands(self, bands: List[str]) -> VectorCube:
        """
        .. versionadded:: 0.22.0
        """
        # TODO #459 docs
        return self.process(
            process_id="filter_bands",
            arguments={"data": THIS, "bands": bands},
        )

    @openeo_process
    def filter_bbox(
        self,
        *,
        west: Optional[float] = None,
        south: Optional[float] = None,
        east: Optional[float] = None,
        north: Optional[float] = None,
        extent: Optional[Union[dict, List[float], Tuple[float, float, float, float], Parameter]] = None,
        crs: Optional[int] = None,
    ) -> VectorCube:
        """
        .. versionadded:: 0.22.0
        """
        # TODO #459 docs
        if any(c is not None for c in [west, south, east, north]):
            if extent is not None:
                raise InvalidBBoxException("Don't specify both west/south/east/north and extent")
            extent = dict_no_none(west=west, south=south, east=east, north=north)

        if isinstance(extent, Parameter):
            pass
        else:
            extent = to_bbox_dict(extent, crs=crs)
        return self.process(
            process_id="filter_bbox",
            arguments={"data": THIS, "extent": extent},
        )

    @openeo_process
    def filter_labels(
        self, condition: Union[PGNode, Callable], dimension: str, context: Optional[dict] = None
    ) -> VectorCube:
        """
        Filters the dimension labels in the data cube for the given dimension.
        Only the dimension labels that match the specified condition are preserved,
        all other labels with their corresponding data get removed.

        :param condition: the "child callback" which will be given a single label value (number or string)
            and returns a boolean expressing if the label should be preserved.
            Also see :ref:`callbackfunctions`.
        :param dimension: The name of the dimension to filter on.

        .. versionadded:: 0.22.0
        """
        condition = build_child_callback(condition, parent_parameters=["value"])
        return self.process(
            process_id="filter_labels",
            arguments=dict_no_none(data=THIS, condition=condition, dimension=dimension, context=context),
        )

    @openeo_process
    def filter_vector(
        self, geometries: Union["VectorCube", shapely.geometry.base.BaseGeometry, dict], relation: str = "intersects"
    ) -> VectorCube:
        """
        .. versionadded:: 0.22.0
        """
        # TODO #459 docs
        if not isinstance(geometries, (VectorCube, Parameter)):
            geometries = self.load_geojson(connection=self.connection, data=geometries)
        return self.process(
            process_id="filter_vector",
            arguments={"data": THIS, "geometries": geometries, "relation": relation},
        )

    @openeo_process
    def fit_class_random_forest(
        self,
        # TODO #279 #293: target type should be `VectorCube` (with adapters for GeoJSON FeatureCollection, GeoPandas, ...)
        target: dict,
        # TODO #293 max_variables officially has no default
        max_variables: Optional[int] = None,
        num_trees: int = 100,
        seed: Optional[int] = None,
    ) -> MlModel:
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

        .. versionadded:: 0.16.0
            Originally added in version 0.10.0 as :py:class:`DataCube <openeo.rest.datacube.DataCube>` method,
            but moved to :py:class:`VectorCube` in version 0.16.0.
        """
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
    ) -> MlModel:
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

        .. versionadded:: 0.16.0
            Originally added in version 0.10.0 as :py:class:`DataCube <openeo.rest.datacube.DataCube>` method,
            but moved to :py:class:`VectorCube` in version 0.16.0.
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
    def apply_dimension(
        self,
        process: Union[str, typing.Callable, UDF, PGNode],
        dimension: str,
        target_dimension: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> VectorCube:
        """
        Applies a process to all values along a dimension of a data cube.
        For example, if the temporal dimension is specified the process will work on the values of a time series.

        The process to apply is specified by providing a callback function in the `process` argument.

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


        :param dimension: The name of the source dimension to apply the process on. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target_dimension: The name of the target dimension or null (the default) to use the source dimension
            specified in the parameter dimension. By specifying a target dimension, the source dimension is removed.
            The target dimension with the specified name and the type other (see add_dimension) is created, if it doesn't exist yet.
        :param context: Additional data to be passed to the process.

        :return: A datacube with the UDF applied to the given dimension.
        :raises: DimensionNotAvailable

        .. versionadded:: 0.22.0
        """
        process = build_child_callback(
            process=process, parent_parameters=["data", "context"], connection=self.connection
        )
        arguments = dict_no_none(
            {
                "data": THIS,
                "process": process,
                "dimension": dimension,
                "target_dimension": target_dimension,
                "context": context,
            }
        )
        return self.process(process_id="apply_dimension", arguments=arguments)

    def vector_to_raster(self, target: openeo.rest.datacube.DataCube) -> openeo.rest.datacube.DataCube:
        """
        Converts this vector cube (:py:class:`VectorCube`) into a raster data cube (:py:class:`~openeo.rest.datacube.DataCube`).
        The bounding polygon of homogenous areas of pixels is constructed.

        :param target: a reference raster data cube to adopt the CRS/projection/resolution from.

        .. warning:: ``vector_to_raster`` is an experimental, non-standard process. It is not widely supported, and its API is subject to change.

        .. versionadded:: 0.28.0

        """
        # TODO: this parameter sniffing is a temporary workaround until
        #       the `target` parameter name rename has fully settled
        #       https://github.com/Open-EO/openeo-python-driver/issues/274
        #       After that has settled, it is still useful to verify assumptions about this non-standard process.
        try:
            process_spec = self.connection.describe_process("vector_to_raster")
            target_parameter = process_spec["parameters"][1]["name"]
            assert "target" in target_parameter
        except Exception:
            target_parameter = "target"

        pg_node = PGNode(
            process_id="vector_to_raster",
            arguments={"data": self, target_parameter: target},
        )
        # TODO: the correct metadata has to be passed here:
        #       replace "geometry" dimension with spatial dimensions of the target cube
        return openeo.rest.datacube.DataCube(pg_node, connection=self._connection, metadata=self.metadata)

import logging
import re
from typing import List, Optional, Union

import numpy
import pandas as pd
import shapely.errors
import shapely.geometry.base
import shapely.wkt

from openeo import BatchJob, Connection
from openeo.internal.processes.parse import (
    Parameter,
    Process,
    parse_remote_process_definition,
)
from openeo.util import LazyLoadCache, repr_truncate

_log = logging.getLogger(__name__)


class ProcessBasedJobCreator:
    """
    Batch job creator
    (to be used together with :py:class:`~openeo.extra.job_management.MultiBackendJobManager`)
    that takes a parameterized openEO process definition
    (e.g a user-defined process (UDP) or a remote openEO process definition),
    and creates a batch job
    for each row of the dataframe managed by the :py:class:`~openeo.extra.job_management.MultiBackendJobManager`
    by filling in the process parameters with corresponding row values.

    .. seealso::
        See :ref:`job-management-with-process-based-job-creator`
        for more information and examples.

    Process parameters are linked to dataframe columns by name.
    While this intuitive name-based matching should cover most use cases,
    there are additional options for overrides or fallbacks:

    -   When provided, ``parameter_column_map`` will be consulted
        for resolving a process parameter name (key in the dictionary)
        to a desired dataframe column name (corresponding value).
    -   One common case is handled automatically as convenience functionality.

        When:

        - ``parameter_column_map`` is not provided (or set to ``None``),
        - and there is a *single parameter* that accepts inline GeoJSON geometries,
        - and the dataframe is a GeoPandas dataframe with a *single geometry* column,

        then this parameter and this geometries column will be linked automatically.

    -   If a parameter can not be matched with a column by name as described above,
        a default value will be picked,
        first by looking in ``parameter_defaults`` (if provided),
        and then by looking up the default value from the parameter schema in the process definition.
    -   Finally if no (default) value can be determined and the parameter
        is not flagged as optional, an error will be raised.


    :param process_id: (optional) openEO process identifier.
        Can be omitted when working with a remote process definition
        that is fully defined with a URL in the ``namespace`` parameter.
    :param namespace: (optional) openEO process namespace.
        Typically used to provide a URL to a remote process definition.
    :param parameter_defaults: (optional) default values for process parameters,
        to be used when not available in the dataframe managed by
        :py:class:`~openeo.extra.job_management.MultiBackendJobManager`.
    :param parameter_column_map: Optional overrides
        for linking process parameters to dataframe columns:
        mapping of process parameter names as key
        to dataframe column names as value.

    .. versionadded:: 0.33.0

    .. warning::
        This is an experimental API subject to change,
        and we greatly welcome
        `feedback and suggestions for improvement <https://github.com/Open-EO/openeo-python-client/issues>`_.

    """

    def __init__(
        self,
        *,
        process_id: Optional[str] = None,
        namespace: Union[str, None] = None,
        parameter_defaults: Optional[dict] = None,
        parameter_column_map: Optional[dict] = None,
    ):
        if process_id is None and namespace is None:
            raise ValueError("At least one of `process_id` and `namespace` should be provided.")
        self._process_id = process_id
        self._namespace = namespace
        self._parameter_defaults = parameter_defaults or {}
        self._parameter_column_map = parameter_column_map
        self._cache = LazyLoadCache()

    def _get_process_definition(self, connection: Connection) -> Process:
        if isinstance(self._namespace, str) and re.match("https?://", self._namespace):
            # Remote process definition handling
            return self._cache.get(
                key=("remote_process_definition", self._namespace, self._process_id),
                load=lambda: parse_remote_process_definition(namespace=self._namespace, process_id=self._process_id),
            )
        elif self._namespace is None:
            # Handling of a user-specific UDP
            udp_raw = connection.user_defined_process(self._process_id).describe()
            return Process.from_dict(udp_raw)
        else:
            raise NotImplementedError(
                f"Unsupported process definition source udp_id={self._process_id!r} namespace={self._namespace!r}"
            )

    def start_job(self, row: pd.Series, connection: Connection, **_) -> BatchJob:
        """
        Implementation of the ``start_job`` callable interface
        of :py:meth:`MultiBackendJobManager.run_jobs`
        to create a job based on given dataframe row

        :param row: The row in the pandas dataframe that stores the jobs state and other tracked data.
        :param connection: The connection to the backend.
        """
        # TODO: refactor out some methods, for better reuse and decoupling:
        #       `get_arguments()` (to build the arguments dictionary), `get_cube()` (to create the cube),

        process_definition = self._get_process_definition(connection=connection)
        process_id = process_definition.id
        parameters = process_definition.parameters or []

        if self._parameter_column_map is None:
            self._parameter_column_map = self._guess_parameter_column_map(parameters=parameters, row=row)

        arguments = {}
        for parameter in parameters:
            param_name = parameter.name
            column_name = self._parameter_column_map.get(param_name, param_name)
            if column_name in row.index:
                # Get value from dataframe row
                value = row.loc[column_name]
            elif param_name in self._parameter_defaults:
                # Fallback on default values from constructor
                value = self._parameter_defaults[param_name]
            elif parameter.has_default():
                # Explicitly use default value from parameter schema
                value = parameter.default
            elif parameter.optional:
                # Skip optional parameters without any fallback default value
                continue
            else:
                raise ValueError(f"Missing required parameter {param_name!r} for process {process_id!r}")

            # Prepare some values/dtypes for JSON encoding
            if isinstance(value, numpy.integer):
                value = int(value)
            elif isinstance(value, numpy.number):
                value = float(value)
            elif isinstance(value, shapely.geometry.base.BaseGeometry):
                value = shapely.geometry.mapping(value)

            arguments[param_name] = value

        cube = connection.datacube_from_process(process_id=process_id, namespace=self._namespace, **arguments)

        title = row.get("title", f"Process {process_id!r} with {repr_truncate(arguments)}")
        description = row.get("description", f"Process {process_id!r} (namespace {self._namespace}) with {arguments}")
        job = connection.create_job(cube, title=title, description=description)

        return job

    def __call__(self, *arg, **kwargs) -> BatchJob:
        """Syntactic sugar for calling :py:meth:`start_job`."""
        return self.start_job(*arg, **kwargs)

    @staticmethod
    def _guess_parameter_column_map(parameters: List[Parameter], row: pd.Series) -> dict:
        """
        Guess parameter-column mapping from given parameter list and dataframe row
        """
        parameter_column_map = {}
        # Geometry based mapping: try to automatically map geometry columns to geojson parameters
        geojson_parameters = [p.name for p in parameters if p.schema.accepts_geojson()]
        geometry_columns = [i for (i, v) in row.items() if isinstance(v, shapely.geometry.base.BaseGeometry)]
        if geojson_parameters and geometry_columns:
            if len(geojson_parameters) == 1 and len(geometry_columns) == 1:
                # Most common case: one geometry parameter and one geometry column: can be mapped naively
                parameter_column_map[geojson_parameters[0]] = geometry_columns[0]
            elif all(p in geometry_columns for p in geojson_parameters):
                # Each geometry param has geometry column with same name: easy to map
                parameter_column_map.update((p, p) for p in geojson_parameters)
            else:
                raise RuntimeError(
                    f"Problem with mapping geometry columns ({geometry_columns}) to process parameters ({geojson_parameters})"
                )
        _log.debug(f"Guessed parameter-column map: {parameter_column_map}")
        return parameter_column_map

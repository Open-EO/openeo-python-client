from __future__ import annotations

import logging
import pathlib
import re
import typing
import uuid
import warnings
from typing import Dict, List, Optional, Tuple, Union

import requests

from openeo.internal.graph_building import FlatGraphableMixin, PGNode, _FromNodeMixin
from openeo.internal.jupyter import render_component
from openeo.internal.processes.builder import (
    convert_callable_to_pgnode,
    get_parameter_names,
)
from openeo.internal.warnings import UserDeprecationWarning
from openeo.rest import OpenEoClientException
from openeo.util import dict_no_none, str_truncate

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection

log = logging.getLogger(__name__)

# Sentinel object to refer to "current" cube in chained cube processing expressions.
THIS = object()


class _ProcessGraphAbstraction(_FromNodeMixin, FlatGraphableMixin):
    """
    Base class for client-side abstractions/wrappers
    for structures that are represented by a openEO process graph:
    raster data cubes, vector cubes, ML models, ...
    """

    def __init__(self, pgnode: PGNode, connection: Union[Connection, None]):
        self._pg = pgnode
        # TODO: now that connection can officially be None:
        #       improve exceptions in cases where is it still assumed to be a real connection (download, create_job, ...)
        self._connection = connection

    def __str__(self):
        return "{t}({pg})".format(t=self.__class__.__name__, pg=self._pg)

    def flat_graph(self) -> Dict[str, dict]:
        """
        Get the process graph in internal flat dict representation.

        .. warning:: This method is mainly intended for internal use.
            It is not recommended for general use and is *subject to change*.

            Instead, it is recommended to use
            :py:meth:`to_json()` or :py:meth:`print_json()`
            to obtain a standardized, interoperable JSON representation of the process graph.
            See :ref:`process_graph_export` for more information.
        """
        # TODO: wrap in {"process_graph":...} by default/optionally?
        return self._pg.flat_graph()

    @property
    def _api_version(self):
        return self._connection.capabilities().api_version_check

    @property
    def connection(self) -> Connection:
        return self._connection

    def result_node(self) -> PGNode:
        """
        Get the current result node (:py:class:`PGNode`) of the process graph.

        .. versionadded:: 0.10.1
        """
        return self._pg

    def from_node(self):
        # _FromNodeMixin API
        return self._pg

    def _build_pgnode(
        self,
        process_id: str,
        arguments: Optional[dict] = None,
        namespace: Optional[str] = None,
        **kwargs
    ) -> PGNode:
        """
        Helper to build a PGNode from given argument dict and/or kwargs,
        and possibly resolving the `THIS` reference.
        """
        arguments = {**(arguments or {}), **kwargs}
        for k, v in arguments.items():
            if v is THIS:
                arguments[k] = self
            # TODO: also necessary to traverse lists/dictionaries?
        return PGNode(process_id=process_id, arguments=arguments, namespace=namespace)

    # TODO #278 also move process graph "execution" methods here: `download`, `execute`, `execute_batch`, `create_job`, `save_udf`,  ...

    def _repr_html_(self):
        process = {"process_graph": self.flat_graph()}
        parameters = {
            "id": uuid.uuid4().hex,
            "explicit-zoom": True,
            "height": "400px",
        }
        return render_component("model-builder", data=process, parameters=parameters)


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

    # TODO: eliminate dependency on `openeo.rest.connection` and move to somewhere under `openeo.internal`?

    __slots__ = ["code", "_runtime", "version", "context", "_source"]

    def __init__(
        self,
        code: str,
        runtime: Optional[str] = None,
        data=None,  # TODO #181 remove `data` argument
        version: Optional[str] = None,
        context: Optional[dict] = None,
        _source=None,
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
        self._runtime = runtime
        self.version = version
        self.context = context
        self._source = _source
        if data is not None:
            # TODO #181 remove `data` argument
            warnings.warn(
                f"The `data` argument of `{self.__class__.__name__}` is deprecated, unused and will be removed in a future release.",
                category=UserDeprecationWarning,
                stacklevel=2,
            )

    def __repr__(self):
        return f"<{type(self).__name__} runtime={self._runtime!r} code={str_truncate(self.code, width=200)!r}>"

    def get_runtime(self, connection: Optional[Connection] = None) -> str:
        return self._runtime or self._guess_runtime(connection=connection)

    @classmethod
    def from_file(
        cls,
        path: Union[str, pathlib.Path],
        runtime: Optional[str] = None,
        version: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> UDF:
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
        return cls(
            code=code, runtime=runtime, version=version, context=context, _source=path
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        runtime: Optional[str] = None,
        version: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> UDF:
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
        return cls(
            code=code, runtime=runtime, version=version, context=context, _source=url
        )

    def _guess_runtime(self, connection: Optional[Connection] = None) -> str:
        """Guess UDF runtime from UDF source (path) or source code."""
        # First, guess UDF language
        language = None
        if isinstance(self._source, pathlib.Path):
            language = self._guess_runtime_from_suffix(self._source.suffix)
        elif isinstance(self._source, str):
            url_match = re.match(
                r"https?://.*?(?P<suffix>\.\w+)([&#].*)?$", self._source
            )
            if url_match:
                language = self._guess_runtime_from_suffix(url_match.group("suffix"))
        if not language:
            # Guess language from UDF code
            if re.search(r"^def [\w0-9_]+\(", self.code, flags=re.MULTILINE):
                language = "Python"
            # TODO: detection heuristics for R and other languages?
        if not language:
            raise OpenEoClientException("Failed to detect language of UDF code.")
        runtime = language
        if connection:
            # Some additional best-effort validation/normalization of the runtime
            # TODO: this just does some case-normalization, just drop that all together to eliminate
            #       the dependency on a connection object. See https://github.com/Open-EO/openeo-api/issues/510
            runtimes = {k.lower(): k for k in connection.list_udf_runtimes().keys()}
            runtime = runtimes.get(runtime.lower(), runtime)
        return runtime

    def _guess_runtime_from_suffix(self, suffix: str) -> Union[str]:
        return {
            ".py": "Python",
            ".r": "R",
        }.get(suffix.lower())

    def get_run_udf_callback(self, connection: Optional[Connection] = None, data_parameter: str = "data") -> PGNode:
        """
        For internal use: construct `run_udf` node to be used as callback in `apply`, `reduce_dimension`, ...
        """
        arguments = dict_no_none(
            data={"from_parameter": data_parameter},
            udf=self.code,
            runtime=self.get_runtime(connection=connection),
            version=self.version,
            context=self.context,
        )
        return PGNode(process_id="run_udf", arguments=arguments)


def build_child_callback(
    process: Union[str, PGNode, typing.Callable, UDF],
    parent_parameters: List[str],
    connection: Optional[Connection] = None,
) -> dict:
    """
    Build a "callback" process: a user defined process that is used by another process (such
    as `apply`, `apply_dimension`, `reduce`, ....)

    :param process: process id string, PGNode or callable that uses the ProcessBuilder mechanism to build a process
    :param parent_parameters: list of parameter names defined for child process
    :param connection: optional connection object to improve runtime validation for UDFs
    :return:
    """
    # TODO: move this to more generic process graph building utility module
    # TODO: autodetect the parameters defined by parent process?
    # TODO: eliminate need for connection object (also see `UDF._guess_runtime`)
    # TODO: when `openeo.rest` deps are gone: move this helper to somewhere under `openeo.internal`
    if isinstance(process, PGNode):
        # Assume this is already a valid callback process
        pg = process
    elif isinstance(process, str):
        # Assume given reducer is a simple predefined reduce process_id
        # TODO: avoid local import (workaround for circular import issue)
        import openeo.processes
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
    elif isinstance(process, dict) and isinstance(process.get("process_graph"), PGNode):
        pg = process["process_graph"]
    else:
        raise ValueError(process)

    return PGNode.to_process_graph_argument(pg)


def _ensure_save_result(
    cube: _ProcessGraphAbstraction,
    *,
    format: Optional[str] = None,
    options: Optional[dict] = None,
    weak_format: Optional[str] = None,
    default_format: str,
    method: str,
) -> _ProcessGraphAbstraction:
    """
    Make sure there is a`save_result` node in the process graph.

    :param format: (optional) desired `save_result` file format
    :param options: (optional) desired `save_result` file format parameters
    :param weak_format: (optional) weak format indicator guessed from file name
    :param default_format: default format for data type to use when no format is specified by user
    :return:
    """
    # TODO #278 instead of standalone helper function, move this to common base class for raster cubes, vector cubes, ...
    save_result_nodes = [n for n in cube.result_node().walk_nodes() if n.process_id == "save_result"]

    if not save_result_nodes:
        # No `save_result` node yet: automatically add it.
        # TODO: the `save_result` method is not defined on _ProcessGraphAbstraction, but it is on DataCube and VectorCube
        cube = cube.save_result(format=format or weak_format or default_format, options=options)
    elif format or options:
        raise OpenEoClientException(
            f"{method} with explicit output {'format' if format else 'options'} {format or options!r},"
            f" but the process graph already has `save_result` node(s)"
            f" which is ambiguous and should not be combined."
        )

    return cube

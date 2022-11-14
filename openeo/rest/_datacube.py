import json
import logging
import sys
import typing
from pathlib import Path
from typing import Optional, Union, Tuple
import uuid

from openeo.internal.compat import nullcontext
from openeo.internal.graph_building import PGNode, _FromNodeMixin
from openeo.internal.jupyter import render_component

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection

log = logging.getLogger(__name__)

# Sentinel object to refer to "current" cube in chained cube processing expressions.
THIS = object()


class _ProcessGraphAbstraction(_FromNodeMixin):
    """
    Base class for client-side abstractions/wrappers
    for structures that are represented by a openEO process graph:
    raster data cubes, vector cubes, ML models, ...
    """

    def __init__(self, pgnode: PGNode, connection: "Connection"):
        self._pg = pgnode
        self._connection = connection

    def __str__(self):
        return "{t}({pg})".format(t=self.__class__.__name__, pg=self._pg)

    def flat_graph(self) -> dict:
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

    def to_json(self, *, indent: Union[int, None] = 2, separators: Optional[Tuple[str, str]] = None) -> str:
        """
        Get interoperable JSON representation of the process graph.

        See :py:meth:`DataCube.print_json` to directly print the JSON representation
        and :ref:`process_graph_export` for more usage information.

        Also see ``json.dumps`` docs for more information on the JSON formatting options.

        :param indent: JSON indentation level.
        :param separators: (optional) tuple of item/key separators.
        :return: JSON string
        """
        pg = {"process_graph": self.flat_graph()}
        return json.dumps(pg, indent=indent, separators=separators)

    def print_json(self, *, file=None, indent: Union[int, None] = 2, separators: Optional[Tuple[str, str]] = None):
        """
        Print interoperable JSON representation of the process graph.

        See :py:meth:`DataCube.to_json` to get the JSON representation as a string
        and :ref:`process_graph_export` for more usage information.

        Also see ``json.dumps`` docs for more information on the JSON formatting options.

        :param file: file-like object (stream) to print to (current ``sys.stdout`` by default).
            Or a path (string or pathlib.Path) to a file to write to.
        :param indent: JSON indentation level.
        :param separators: (optional) tuple of item/key separators.

        .. versionadded:: 0.12.0
        """
        pg = {"process_graph": self.flat_graph()}
        if isinstance(file, (str, Path)):
            # Create (new) file and automatically close it
            file_ctx = Path(file).open("w", encoding="utf8")
        else:
            # Just use file as-is, but don't close it automatically.
            file_ctx = nullcontext(enter_result=file or sys.stdout)
        with file_ctx as f:
            json.dump(pg, f, indent=indent, separators=separators)
            if indent is not None:
                f.write("\n")

    @property
    def _api_version(self):
        return self._connection.capabilities().api_version_check

    @property
    def connection(self) -> "Connection":
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

    def _build_pgnode(self, process_id: str, arguments: dict, namespace: Optional[str], **kwargs) -> PGNode:
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
            'id': uuid.uuid4().hex,
            'explicit-zoom': True,
            'height': '400px'
        }
        return render_component('model-builder', data = process, parameters = parameters)

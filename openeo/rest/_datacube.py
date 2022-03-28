import json
import logging
import typing
from typing import Optional

from openeo.internal.graph_building import PGNode, _FromNodeMixin
from openeo.util import legacy_alias

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime). `hasattr` is Python 3.5 workaround #210
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
        Get the process graph in flat dict representation

        .. note:: This method is mainly for internal use, subject to change and not recommended for general usage.
            Instead, use :py:meth:`to_json()` to get a JSON representation of the process graph.
        """
        # TODO: wrap in {"process_graph":...} by default/optionally?
        return self._pg.flat_graph()

    flatten = legacy_alias(flat_graph, name="flatten")

    def to_json(self, indent=2, separators=None) -> str:
        """
        Get JSON representation of (flat dict) process graph.
        """
        pg = {"process_graph": self.flat_graph()}
        return json.dumps(pg, indent=indent, separators=separators)

    @property
    def _api_version(self):
        return self._connection.capabilities().api_version_check

    @property
    def connection(self) -> "Connection":
        return self._connection

    def result_node(self):
        """Get the result node (:py:class:`PGNode`) of the process graph."""
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

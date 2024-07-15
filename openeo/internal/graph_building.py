"""
Internal openEO process graph building utilities
''''''''''''''''''''''''''''''''''''''''''''''''''

Internal functionality for abstracting, building, manipulating and processing openEO process graphs.

"""

from __future__ import annotations

import abc
import collections
import json
import sys
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from openeo.api.process import Parameter
from openeo.internal.process_graph_visitor import (
    ProcessGraphUnflattener,
    ProcessGraphVisitException,
    ProcessGraphVisitor,
)
from openeo.util import dict_no_none, load_json_resource


class FlatGraphableMixin(metaclass=abc.ABCMeta):
    """
    Mixin for classes that can be exported/converted to
    a "flat graph" representation of an openEO process graph.
    """

    @abc.abstractmethod
    def flat_graph(self) -> Dict[str, dict]:
        ...

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

    def print_json(
        self,
        *,
        file=None,
        indent: Union[int, None] = 2,
        separators: Optional[Tuple[str, str]] = None,
        end: str = "\n",
    ):
        """
        Print interoperable JSON representation of the process graph.

        See :py:meth:`DataCube.to_json` to get the JSON representation as a string
        and :ref:`process_graph_export` for more usage information.

        Also see ``json.dumps`` docs for more information on the JSON formatting options.

        :param file: file-like object (stream) to print to (current ``sys.stdout`` by default).
            Or a path (string or pathlib.Path) to a file to write to.
        :param indent: JSON indentation level.
        :param separators: (optional) tuple of item/key separators.
        :param end: additional string to be printed at the end (newline by default).

        .. versionadded:: 0.12.0

        .. versionadded:: 0.23.0
            added the ``end`` argument.
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
            if end:
                f.write(end)


class _FromNodeMixin(abc.ABC):
    """Mixin for classes that want to hook into the generation of a "from_node" reference."""

    @abc.abstractmethod
    def from_node(self) -> PGNode:
        # TODO: "from_node" is a bit a confusing name:
        #       it refers to the "from_node" node reference in openEO process graphs,
        #       but as a method name here it reads like "construct from PGNode",
        #       while it is actually meant as "export as PGNode" (that can be used in a "from_node" reference).
        pass


class PGNode(_FromNodeMixin, FlatGraphableMixin):
    """
    A process node in a process graph: has at least a process_id and arguments.

    Note that a full openEO "process graph" is essentially a directed acyclic graph of nodes
    pointing to each other. A full process graph is practically equivalent with its "result" node,
    as it points (directly or indirectly) to all the other nodes it depends on.

    .. warning::
        This class is an implementation detail meant for internal use.
        It is not recommended for general use in normal user code.
        Instead, use process graph abstraction builders like
        :py:meth:`Connection.load_collection() <openeo.rest.connection.Connection.load_collection>`,
        :py:meth:`Connection.datacube_from_process() <openeo.rest.connection.Connection.datacube_from_process>`,
        :py:meth:`Connection.datacube_from_flat_graph() <openeo.rest.connection.Connection.datacube_from_flat_graph>`,
        :py:meth:`Connection.datacube_from_json() <openeo.rest.connection.Connection.datacube_from_json>`,
        :py:meth:`Connection.load_ml_model() <openeo.rest.connection.Connection.load_ml_model>`,
        :py:func:`openeo.processes.process()`,

    """

    __slots__ = ["_process_id", "_arguments", "_namespace"]

    def __init__(self, process_id: str, arguments: dict = None, namespace: Union[str, None] = None, **kwargs):
        self._process_id = process_id
        # Merge arguments dict and kwargs
        arguments = dict(**(arguments or {}), **kwargs)
        # Make sure direct PGNode arguments are properly wrapped in a "from_node" dict
        for arg, value in arguments.items():
            if isinstance(value, _FromNodeMixin):
                arguments[arg] = {"from_node": value.from_node()}
            elif isinstance(value, list):
                for index, arrayelement in enumerate(value):
                    if isinstance(arrayelement, _FromNodeMixin):
                        value[index] = {"from_node": arrayelement.from_node()}
        # TODO: use a frozendict of some sort to ensure immutability?
        self._arguments = arguments
        self._namespace = namespace

    def from_node(self):
        return self

    def __repr__(self):
        return "<{c} {p!r} at 0x{m:x}>".format(c=self.__class__.__name__, p=self.process_id, m=id(self))

    @property
    def process_id(self) -> str:
        return self._process_id

    @property
    def arguments(self) -> dict:
        return self._arguments

    @property
    def namespace(self) -> Union[str, None]:
        return self._namespace

    def update_arguments(self, **kwargs):
        """
        Add/Update arguments of the process node.

        .. versionadded:: 0.10.1
        """
        self._arguments = {**self._arguments, **kwargs}

    def _as_tuple(self):
        return (self._process_id, self._arguments, self._namespace)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self._as_tuple() == other._as_tuple()

    def to_dict(self) -> dict:
        """
        Convert process graph to a nested dictionary structure.
        Uses deep copy style: nodes that are reused in graph will be deduplicated
        """

        def _deep_copy(x):
            """PGNode aware deep copy helper"""
            if isinstance(x, PGNode):
                return dict_no_none(process_id=x.process_id, arguments=_deep_copy(x.arguments), namespace=x.namespace)
            if isinstance(x, Parameter):
                return {"from_parameter": x.name}
            elif isinstance(x, dict):
                return {str(k): _deep_copy(v) for k, v in x.items()}
            elif isinstance(x, (list, tuple)):
                return type(x)(_deep_copy(v) for v in x)
            elif isinstance(x, (str, int, float)) or x is None:
                return x
            else:
                raise ValueError(repr(x))

        return _deep_copy(self)

    def flat_graph(self) -> Dict[str, dict]:
        """Get the process graph in internal flat dict representation."""
        return GraphFlattener().flatten(node=self)

    @staticmethod
    def to_process_graph_argument(value: Union["PGNode", str, dict]) -> dict:
        """
        Normalize given argument properly to a "process_graph" argument
        to be used as reducer/subprocess for processes like
        ``reduce_dimension``, ``aggregate_spatial``, ``apply``, ``merge_cubes``, ``resample_cube_temporal``
        """
        if isinstance(value, str):
            # assume string with predefined reduce/apply process ("mean", "sum", ...)
            # TODO: is this case still used? It's invalid anyway for 1.0 openEO spec I think?
            return value
        elif isinstance(value, PGNode):
            return {"process_graph": value}
        elif isinstance(value, dict) and isinstance(value.get("process_graph"), PGNode):
            return value
        else:
            raise ValueError(value)

    @staticmethod
    def from_flat_graph(flat_graph: dict, parameters: Optional[dict] = None) -> PGNode:
        """Unflatten a given flat dict representation of a process graph and return result node."""
        return PGNodeGraphUnflattener.unflatten(flat_graph=flat_graph, parameters=parameters)


def as_flat_graph(x: Union[dict, FlatGraphableMixin, Path, Any]) -> Dict[str, dict]:
    """
    Convert given object to a internal flat dict graph representation.
    """
    # TODO: document or verify which process graph flavor this is:
    #       including `{"process": {"process_graph": {nodes}}` ("process graph with metadata")
    #       including `{"process_graph": {nodes}}` ("process graph")
    #       or just the raw process graph nodes?
    if isinstance(x, dict):
        return x
    elif isinstance(x, FlatGraphableMixin):
        return x.flat_graph()
    elif isinstance(x, (str, Path)):
        # Assume a JSON resource (raw JSON, path to local file, JSON url, ...)
        return load_json_resource(x)
    raise ValueError(x)


class ReduceNode(PGNode):
    """
    A process graph node for "reduce" processes (has a reducer sub-process-graph)
    """

    def __init__(
        self,
        data: _FromNodeMixin,
        reducer: Union[PGNode, str, dict],
        dimension: str,
        context=None,
        process_id="reduce_dimension",
        band_math_mode: bool = False,
    ):
        assert process_id in ("reduce_dimension", "reduce_dimension_binary")
        arguments = {
            "data": data,
            "reducer": self.to_process_graph_argument(reducer),
            "dimension": dimension,
        }
        if context is not None:
            arguments["context"] = context
        super().__init__(process_id=process_id, arguments=arguments)
        # TODO #123 is it (still) necessary to make "band" math a special case?
        self.band_math_mode = band_math_mode

    @property
    def dimension(self):
        return self.arguments["dimension"]

    def reducer_process_graph(self) -> PGNode:
        return self.arguments["reducer"]["process_graph"]

    def clone_with_new_reducer(self, reducer: PGNode) -> ReduceNode:
        """Copy/clone this reduce node: keep input reference, but use new reducer"""
        return ReduceNode(
            data=self.arguments["data"]["from_node"],
            reducer=reducer,
            dimension=self.arguments["dimension"],
            band_math_mode=self.band_math_mode,
            context=self.arguments.get("context"),
        )


class FlatGraphNodeIdGenerator:
    """
    Helper class to generate unique node ids (e.g. autoincrement style)
    for processes in a flat process graph.
    """

    def __init__(self):
        self._counters = collections.defaultdict(int)

    def generate(self, process_id: str):
        """Generate new key for given process id."""
        self._counters[process_id] += 1
        return "{p}{c}".format(p=process_id.replace("_", ""), c=self._counters[process_id])


class GraphFlattener(ProcessGraphVisitor):

    def __init__(self, node_id_generator: FlatGraphNodeIdGenerator = None):
        super().__init__()
        self._node_id_generator = node_id_generator or FlatGraphNodeIdGenerator()
        self._last_node_id = None
        self._flattened: Dict[str, dict] = {}
        self._argument_stack = []
        self._node_cache = {}

    def flatten(self, node: PGNode) -> Dict[str, dict]:
        """Consume given nested process graph and return flat dict representation"""
        self.accept_node(node)
        assert len(self._argument_stack) == 0
        self._flattened[self._last_node_id]["result"] = True
        return self._flattened

    def accept_node(self, node: PGNode):
        # Process reused nodes only first time and remember node id.
        node_id = id(node)
        if node_id not in self._node_cache:
            super()._accept_process(process_id=node.process_id, arguments=node.arguments, namespace=node.namespace)
            self._node_cache[node_id] = self._last_node_id
        else:
            self._last_node_id = self._node_cache[node_id]

    def enterProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        self._argument_stack.append({})

    def leaveProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        node_id = self._node_id_generator.generate(process_id)
        self._flattened[node_id] = dict_no_none(
            process_id=process_id,
            arguments=self._argument_stack.pop(),
            namespace=namespace,
        )
        self._last_node_id = node_id

    def _store_argument(self, argument_id: str, value):
        if isinstance(value, Parameter):
            value = {"from_parameter": value.name}
        self._argument_stack[-1][argument_id] = value

    def _store_array_element(self, value):
        if isinstance(value, Parameter):
            value = {"from_parameter": value.name}
        self._argument_stack[-1].append(value)

    def enterArray(self, argument_id: str):
        array = []
        self._store_argument(argument_id, array)
        self._argument_stack.append(array)

    def leaveArray(self, argument_id: str):
        self._argument_stack.pop()

    def arrayElementDone(self, value):
        self._store_array_element(self._flatten_argument(value))

    def constantArrayElement(self, value):
        self._store_array_element(self._flatten_argument(value))

    def _flatten_argument(self, value):
        if isinstance(value, dict):
            if "from_node" in value:
                value = {"from_node": self._last_node_id}
            elif "process_graph" in value:
                pg = value["process_graph"]
                if isinstance(pg, PGNode):
                    value = {"process_graph": GraphFlattener(node_id_generator=self._node_id_generator).flatten(pg)}
                elif isinstance(pg, dict):
                    # Assume it is already a valid flat graph representation of a subprocess
                    value = {"process_graph": pg}
                else:
                    raise ValueError(pg)
            else:
                value = {k: self._flatten_argument(v) for k, v in value.items()}
        elif isinstance(value, Parameter):
            value = {"from_parameter": value.name}
        return value

    def leaveArgument(self, argument_id: str, value):
        self._store_argument(argument_id, self._flatten_argument(value))

    def constantArgument(self, argument_id: str, value):
        self._store_argument(argument_id, value)


class PGNodeGraphUnflattener(ProcessGraphUnflattener):
    """
    Unflatten a flat process graph to a graph of :py:class:`PGNode` objects

    Parameter substitution can also be performed, but is optional:
    if the ``parameters=None`` is given, no parameter substitution is done,
    if it is a dictionary (even an empty one) is given, every parameter encountered in the process
    graph must have an entry for substitution.
    """

    def __init__(self, flat_graph: dict, parameters: Optional[dict] = None):
        super().__init__(flat_graph=flat_graph)
        self._parameters = parameters

    def _process_node(self, node: dict) -> PGNode:
        return PGNode(
            process_id=node["process_id"],
            arguments=self._process_value(value=node["arguments"]),
            namespace=node.get("namespace"),
        )

    def _process_from_node(self, key: str, node: dict) -> PGNode:
        return self.get_node(key=key)

    def _process_from_parameter(self, name: str) -> Any:
        if self._parameters is None:
            return super()._process_from_parameter(name=name)
        if name not in self._parameters:
            raise ProcessGraphVisitException("No substitution value for parameter {p!r}.".format(p=name))
        return self._parameters[name]

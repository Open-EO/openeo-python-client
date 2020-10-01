"""

Process graph building functionality for 1.0.0-style process graphs and DataCube

"""
import collections
from typing import Union, Dict

from openeo import ImageCollection
from openeo.internal.process_graph_visitor import ProcessGraphVisitor


class PGNode:
    """
    Wrapper for process node in a process graph (has process_id and arguments).

    While this is a simple, thin container, it allows a bit more abstraction, basic encapsulation,
    type hinting and code intelligence in your IDE than something generic like a dict.

    Also note that a full openEO "process graph" is essentially a directed acyclic graph of nodes
    pointing to each other. A process graph is practically equivalent with its "result" node,
    as it points (directly or recursively) to all the other nodes it depends on.

    """

    def __init__(self, process_id: str, arguments: dict = None, **kwargs):
        self._process_id = process_id
        # Merge arguments dict and kwargs
        arguments = dict(**(arguments or {}), **kwargs)
        # Make sure direct PGNode arguments are properly wrapped in a "from_node" dict
        for arg, value in arguments.items():
            if isinstance(value, PGNode):
                arguments[arg] = {"from_node": value}
        # TODO: use a frozendict of some sort to ensure immutability?
        self._arguments = arguments

    def __repr__(self):
        return "<{c} {p!r} at 0x{m:x}>".format(c=self.__class__.__name__, p=self.process_id, m=id(self))

    @property
    def process_id(self) -> str:
        return self._process_id

    @property
    def arguments(self) -> dict:
        return self._arguments

    def to_dict(self) -> dict:
        """
        Convert process graph to a nested dictionary structure.
        Uses deep copy style: nodes that are reused in graph will be deduplicated
        """

        def _deep_copy(x):
            """PGNode aware deep copy helper"""
            if isinstance(x, PGNode):
                return {"process_id": x.process_id, "arguments": _deep_copy(x.arguments)}
            elif isinstance(x, dict):
                return {str(k): _deep_copy(v) for k, v in x.items()}
            elif isinstance(x, (list, tuple)):
                return type(x)(_deep_copy(v) for v in x)
            elif isinstance(x, (str, int, float)) or x is None:
                return x
            else:
                raise ValueError(repr(x))

        return _deep_copy(self)

    def flatten(self):
        # First convert to dict (as deep copy)
        return GraphFlattener().flatten(node=self)

    @staticmethod
    def to_process_graph_argument(value: Union['PGNode', str, dict]) -> dict:
        """
        Normalize given argument properly to a "process_graph" argument
        to be used as reducer/subprocess for processes like
        'reduce_dimension', 'aggregate_spatial', 'apply', 'merge_cubes', 'resample_cube_temporal'
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


class UDF(PGNode):
    """
    A 'run_udf' process graph node. This is offered as a convenient way to construct run_udf processes.
    """

    def __init__(self, code:str,runtime:str,data=None,version:str = None,context:Dict=None):
        from openeo.rest.datacube import THIS
        if data==None:
            data=THIS
        arguments = {
            "data": data,
            "udf": code,
            "runtime": runtime
        }
        if version is not None:
            arguments["version"] = version

        if context is not None:
            arguments["context"] = context

        super().__init__(process_id='run_udf', arguments=arguments)


class ReduceNode(PGNode):
    """
    A process graph node for "reduce" processes (has a reducer sub-process-graph)
    """

    def __init__(self, data: PGNode, reducer: Union[PGNode, str, dict], dimension: str, process_id="reduce_dimension",
                 band_math_mode: bool = False):
        assert process_id in ("reduce_dimension", "reduce_dimension_binary")
        arguments = {
            "data": {"from_node": data},
            "reducer": self.to_process_graph_argument(reducer),
            "dimension": dimension,
            # TODO #125 context
        }
        super().__init__(process_id=process_id, arguments=arguments)
        # TODO #123 is it (still) necessary to make "band" math a special case?
        self.band_math_mode = band_math_mode

    @property
    def dimension(self):
        return self.arguments["dimension"]

    def reducer_process_graph(self) -> PGNode:
        return self.arguments["reducer"]["process_graph"]

    def clone_with_new_reducer(self, reducer: PGNode) -> 'ReduceNode':
        """Copy/clone this reduce node: keep input reference, but use new reducer"""
        return ReduceNode(
            data=self.arguments["data"]["from_node"],
            reducer=reducer,
            dimension=self.arguments["dimension"],
            band_math_mode=self.band_math_mode
            # TODO: context?
        )


class FlatGraphNodeIdGenerator:
    """
    Helper class to generate unique node ids (e.g. autoincrement style)
    for processes in a flattened process graph.
    """

    def __init__(self):
        self._counters = collections.defaultdict(int)

    def generate(self, process_id: str):
        """Generate new key for given process id."""
        self._counters[process_id] += 1
        return "{p}{c}".format(p=process_id.replace('_', ''), c=self._counters[process_id])


class GraphFlattener(ProcessGraphVisitor):

    def __init__(self, node_id_generator: FlatGraphNodeIdGenerator = None):
        super().__init__()
        self._node_id_generator = node_id_generator or FlatGraphNodeIdGenerator()
        self._last_node_id = None
        self._flattened = {}
        self._argument_stack = []
        self._node_cache = {}

    def flatten(self, node: PGNode):
        """Consume given nested process graph and return flattened version"""
        self.accept_node(node)
        assert len(self._argument_stack) == 0
        self._flattened[self._last_node_id]["result"] = True
        return self._flattened

    def accept_node(self, node: PGNode):
        # Process reused nodes only first time and remember node id.
        node_id = id(node)
        if node_id not in self._node_cache:
            super()._accept_process(process_id=node.process_id, arguments=node.arguments)
            self._node_cache[node_id] = self._last_node_id
        else:
            self._last_node_id = self._node_cache[node_id]

    def enterProcess(self, process_id: str, arguments: dict):
        self._argument_stack.append({})

    def leaveProcess(self, process_id: str, arguments: dict):
        node_id = self._node_id_generator.generate(process_id)
        self._flattened[node_id] = {
            "process_id": process_id,
            "arguments": self._argument_stack.pop()
        }
        self._last_node_id = node_id

    def _store_argument(self, argument_id: str, value):
        self._argument_stack[-1][argument_id] = value

    def _store_array_element(self, value):
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
                value = {"process_graph": GraphFlattener(node_id_generator=self._node_id_generator).flatten(pg)}
            else:
                value = {k: self._flatten_argument(v) for k, v in value.items()}
        return value

    def leaveArgument(self, argument_id: str, value):
        self._store_argument(argument_id, self._flatten_argument(value))

    def constantArgument(self, argument_id: str, value):
        self._store_argument(argument_id, value)

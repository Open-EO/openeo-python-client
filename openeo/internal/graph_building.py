"""

Process graph building functionality for 1.0.0-style process graphs and DataCube

"""
import collections
import copy
from typing import Dict, Union

from openeo.internal.process_graph_visitor import ProcessGraphVisitor


class PGNode:
    """
    Generic node in a process graph.

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

    @classmethod
    def _deep_copy(cls, x):
        """PGNode aware deep copy helper"""
        if isinstance(x, PGNode):
            return {"process_id": x.process_id, "arguments": cls._deep_copy(x.arguments)}
        elif isinstance(x, dict):
            return {str(k): cls._deep_copy(v) for k, v in x.items()}
        elif isinstance(x, (list, tuple)):
            return type(x)(cls._deep_copy(v) for v in x)
        elif isinstance(x, (str, int, float)) or x is None:
            return x
        else:
            raise ValueError(repr(x))

    def to_dict(self) -> dict:
        """Convert process graph to a nested dictionary (deep copy)"""
        return self._deep_copy(self)

    def flatten(self):
        # First convert to dict (as deep copy)
        return GraphFlattener().flatten(graph=self.to_dict())


class ReduceNode(PGNode):
    """
    A process graph node for "reduce" processes (has a reducer sub-process-graph)
    """

    def __init__(self, data: PGNode, reducer: Union[PGNode, str], dimension: str, process_id="reduce_dimension"):
        assert process_id in ("reduce_dimension", "reduce_dimension_binary")
        super(ReduceNode, self).__init__(
            process_id=process_id,
            arguments={
                "data": {"from_node": data},
                "reducer": reducer if isinstance(reducer, str) else {"process_graph": reducer},
                "dimension": dimension,
                # TODO context
            }
        )

    def reducer_process_graph(self) -> PGNode:
        return self.arguments["reducer"]["process_graph"]

    def clone_with_new_reducer(self, reducer: PGNode):
        """Copy/clone this reduce node: keep input reference, but use new reducer"""
        return ReduceNode(
            data=self.arguments["data"]["from_node"],
            reducer=reducer,
            dimension=self.arguments["dimension"],
            # TODO: context?
        )

    def is_bandmath(self) -> bool:
        # TODO: avoid hardcoded "spectral_bands" dimension #76 #93 #116
        return self.arguments["dimension"] == "spectral_bands"


class FlatGraphKeyGenerator:
    """
    Helper class to generate unique keys (e.g. autoincrement style)
    for processes in a flattened process graph.
    """

    def __init__(self):
        self._counters = collections.defaultdict(int)

    def generate(self, process_id: str):
        """Generate new key for given process id."""
        self._counters[process_id] += 1
        return "{p}{c}".format(p=process_id.replace('_', ''), c=self._counters[process_id])


class GraphFlattener(ProcessGraphVisitor):

    def __init__(self, key_generator: FlatGraphKeyGenerator = None):
        super().__init__()
        self._key_generator = key_generator or FlatGraphKeyGenerator()
        self._last_node_id = None
        self._flattened = {}

    def flatten(self, graph: dict):
        """Consume given nested process graph and return flattened version"""
        # take a copy, flattener modifies the graph in-place
        self.accept(graph)
        self._flattened[self._last_node_id]["result"] = True
        return self._flattened

    def leaveProcess(self, process_id, arguments: Dict):
        node_id = self._key_generator.generate(process_id)
        self._last_node_id = node_id
        self._flattened[node_id] = {
            'process_id': process_id,
            'arguments': arguments
        }

    def arrayElementDone(self, node):
        if 'from_node' in node:
            node['from_node'] = self._last_node_id

    def leaveArgument(self, argument_id, node: Dict):
        if 'from_node' in node:
            node['from_node'] = self._last_node_id
        if isinstance(node, dict) and 'process_graph' in node:
            callback = node['process_graph']
            flat_callback = GraphFlattener(key_generator=self._key_generator).flatten(callback)
            node['process_graph'] = flat_callback

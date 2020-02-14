import collections
import copy
from typing import Dict, Union

from openeo.internal.process_graph_visitor import ProcessGraphVisitor


class GraphBuilder:
    """
    Graph builder for process graphs compatible with openEO API version 1.0.0
    """

    # TODO the only state in this class is `result_node`: why not just define some kind of "graph node" class (instead of "builder")?

    def __init__(self, graph=None):
        """
            Create a process graph builder.
            If a graph is provided, its nodes will be added to this builder, this does not necessarily preserve id's of the nodes.

            :param graph: Dict : Optional, existing process graph
        """
        if graph is not None:
            # TODO: this `graph` argument is unused? Is it useful anyway?
            self.result_node = graph.result_node
        # TODO: what is result_node in "else" case?

    def shallow_copy(self):
        """
        Copy, but don't update keys
        :return:
        """
        # TODO can we avoid copies and work with immutable structures?
        the_copy = GraphBuilder()
        the_copy.result_node = copy.deepcopy(self.result_node)
        return the_copy

    @classmethod
    def from_process_graph(cls, graph: Dict):
        # TODO Can't this just be part of default constructor?
        # TODO: can we avoid the deepcopy?
        # TODO is this a nested or flat graph?
        builder = GraphBuilder()
        builder.result_node = copy.deepcopy(graph)
        return builder

    def add_process(self, process_id: str, arguments: dict = None, **kwargs):
        """
        Add a process to the graph

        :param process_id: process id
        :param arguments: dictionary of process arguments (can also be provided through kwargs)
        """
        if arguments and kwargs:
            raise ValueError("At most one of `arguments` and `kwargs` should be specified")
        arguments = arguments or kwargs
        new_process = {
            'process_id': process_id,
            'arguments': arguments
        }
        self.result_node = new_process

    @classmethod
    def combine(cls, operator: str, first: Union['GraphBuilder', dict], second: Union['GraphBuilder', dict], arg_name='data'):
        """Combine two GraphBuilders to a new merged one using the given operator"""
        merged = cls()

        args = {
            arg_name:[{'from_node':first.result_node}, {'from_node':second.result_node}]
        }

        merged.add_process(operator, **args)
        return merged

    def flatten(self):
        return GraphFlattener().flatten(graph=self.result_node)


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
        # TODO: can we avoind in-place editing?
        self.accept(copy.deepcopy(graph))
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
        if isinstance(node, dict) and 'callback' in node:
            callback = node['callback']
            flat_callback = GraphFlattener(key_generator=self._key_generator).flatten(callback)
            node['callback'] = flat_callback

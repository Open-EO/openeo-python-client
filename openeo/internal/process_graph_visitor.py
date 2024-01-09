from __future__ import annotations

import json
from abc import ABC
from typing import Any, Tuple, Union

from openeo.internal.warnings import deprecated
from openeo.rest import OpenEoClientException


class ProcessGraphVisitException(OpenEoClientException):
    pass


class ProcessGraphVisitor(ABC):
    """
    Hierarchical Visitor for (nested) process graphs structures.
    """

    def __init__(self):
        self.process_stack = []

    @classmethod
    def dereference_from_node_arguments(cls, process_graph: dict) -> str:
        """
        Walk through the given (flat) process graph and replace (in-place) "from_node" references in
        process arguments (dictionaries or lists) with the corresponding resolved subgraphs

        :param process_graph: process graph dictionary to be manipulated in-place
        :return: name of the "result" node of the graph
        """

        # TODO avoid manipulating process graph in place? make it more explicit? work on a copy?
        # TODO call it more something like "unflatten"?. Split this off of ProcessGraphVisitor?
        # TODO implement this through `ProcessGraphUnflattener` ?

        def resolve_from_node(process_graph, node, from_node):
            if from_node not in process_graph:
                raise ProcessGraphVisitException(
                    "from_node {f!r} (referenced by {n!r}) not in process graph.".format(f=from_node, n=node)
                )
            return process_graph[from_node]

        result_node = None
        for node, node_dict in process_graph.items():
            if node_dict.get("result", False):
                if result_node:
                    raise ProcessGraphVisitException("Multiple result nodes: {a}, {b}".format(a=result_node, b=node))
                result_node = node
            arguments = node_dict.get("arguments", {})
            for arg in arguments.values():
                if isinstance(arg, dict):
                    if "from_node" in arg:
                        arg["node"] = resolve_from_node(process_graph, node, arg["from_node"])
                    else:
                        for k, v in arg.items():
                            if isinstance(v, dict) and "from_node" in v:
                                v["node"] = resolve_from_node(process_graph, node, v["from_node"])
                elif isinstance(arg, list):
                    for i, element in enumerate(arg):
                        if isinstance(element, dict) and "from_node" in element:
                            arg[i] = resolve_from_node(process_graph, node, element["from_node"])

        if result_node is None:
            dump = json.dumps(process_graph, indent=2)
            raise ProcessGraphVisitException("No result node in process graph: " + dump[:1000])
        return result_node

    def accept_process_graph(self, graph: dict) -> ProcessGraphVisitor:
        """
        Traverse a (flat) process graph

        :param graph:
        :return:
        """
        # TODO: this is driver specific functionality, working on flattened graph structures. Make this more clear?
        top_level_node = self.dereference_from_node_arguments(graph)
        self.accept_node(graph[top_level_node])
        return self

    @deprecated(reason="Use accept_node() instead", version="0.4.6")
    def accept(self, node: dict):
        self.accept_node(node)

    def accept_node(self, node: dict):
        pid = node["process_id"]
        arguments = node.get("arguments", {})
        namespace = node.get("namespace", None)
        self._accept_process(process_id=pid, arguments=arguments, namespace=namespace)

    def _accept_process(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        self.process_stack.append(process_id)
        self.enterProcess(process_id=process_id, arguments=arguments, namespace=namespace)
        for arg_id, value in sorted(arguments.items()):
            if isinstance(value, list):
                self.enterArray(argument_id=arg_id)
                self._accept_argument_list(value)
                self.leaveArray(argument_id=arg_id)
            elif isinstance(value, dict):
                self.enterArgument(argument_id=arg_id, value=value)
                self._accept_argument_dict(value)
                self.leaveArgument(argument_id=arg_id, value=value)
            else:
                self.constantArgument(argument_id=arg_id, value=value)
        self.leaveProcess(process_id=process_id, arguments=arguments, namespace=namespace)
        assert self.process_stack.pop() == process_id

    def _accept_argument_list(self, elements: list):
        for element in elements:
            if isinstance(element, dict):
                self._accept_argument_dict(element)
                self.arrayElementDone(element)
            else:
                self.constantArrayElement(element)

    def _accept_argument_dict(self, value: dict):
        if "node" in value and "from_node" in value:
            # TODO: this looks bit weird (or at least very specific).
            self.accept_node(value["node"])
        elif value.get("from_node"):
            self.accept_node(value["from_node"])
        elif "process_id" in value:
            self.accept_node(value)
        elif "from_parameter" in value:
            self.from_parameter(value["from_parameter"])
        else:
            self._accept_dict(value)

    def _accept_dict(self, value: dict):
        pass

    def from_parameter(self, parameter_id: str):
        pass

    def enterProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        pass

    def leaveProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        pass

    def enterArgument(self, argument_id: str, value):
        pass

    def leaveArgument(self, argument_id: str, value):
        pass

    def constantArgument(self, argument_id: str, value):
        pass

    def enterArray(self, argument_id: str):
        pass

    def leaveArray(self, argument_id: str):
        pass

    def constantArrayElement(self, value):
        pass

    def arrayElementDone(self, value: dict):
        pass


def find_result_node(flat_graph: dict) -> Tuple[str, dict]:
    """
    Find result node in flat graph

    :return: tuple with node id (str) and node dictionary of the result node.
    """
    result_nodes = [(key, node) for (key, node) in flat_graph.items() if node.get("result")]

    if len(result_nodes) == 1:
        return result_nodes[0]
    elif len(result_nodes) == 0:
        raise ProcessGraphVisitException("Found no result node in flat process graph")
    else:
        keys = [k for (k, n) in result_nodes]
        raise ProcessGraphVisitException(
            "Found multiple result nodes in flat process graph: {keys!r}".format(keys=keys)
        )


class ProcessGraphUnflattener:
    """
    Base class to process a flat graph representation of a process graph
    and unflatten it by resolving the "from_node" references.
    Subclassing and overriding certain methods allows to build a desired unflattened graph structure.
    """

    # Sentinel object for flagging a node "under construction" and detect graph cycles.
    _UNDER_CONSTRUCTION = object()

    def __init__(self, flat_graph: dict):
        self._flat_graph = flat_graph
        self._nodes = {}

    @classmethod
    def unflatten(cls, flat_graph: dict, **kwargs):
        """Class method helper to unflatten given flat process graph"""
        return cls(flat_graph=flat_graph, **kwargs).process()

    def process(self):
        """Process the flat process graph: unflatten it."""
        result_key, result_node = find_result_node(flat_graph=self._flat_graph)
        return self.get_node(result_key)

    def get_node(self, key: str) -> Any:
        """Get processed node by node key."""
        if key not in self._nodes:
            self._nodes[key] = self._UNDER_CONSTRUCTION
            node = self._process_node(self._flat_graph[key])
            self._nodes[key] = node
        elif self._nodes[key] is self._UNDER_CONSTRUCTION:
            raise ProcessGraphVisitException("Cycle in process graph")
        return self._nodes[key]

    def _process_node(self, node: dict) -> Any:
        """
        Overridable: generate process graph node from flat_graph data.
        """
        # Default implementation: basic validation/whitelisting, and only traverse arguments
        return dict(
            process_id=node["process_id"],
            arguments=self._process_value(value=node["arguments"]),
            **{k: node[k] for k in ["namespace", "description", "result"] if k in node},
        )

    def _process_from_node(self, key: str, node: dict) -> Any:
        """
        Overridable: generate a node from a flat_graph "from_node" reference
        """
        # Default/original implementation: keep "from_node" key and add resolved node under "node" key.
        # TODO: just return `self.get_node(key=key)`
        return {"from_node": key, "node": self.get_node(key=key)}

    def _process_from_parameter(self, name: str) -> Any:
        """
        Overridable: generate a node from a flat_graph "from_parameter" reference
        """
        # Default implementation:
        return {"from_parameter": name}

    def _resolve_from_node(self, key: str) -> dict:
        if key not in self._flat_graph:
            raise ProcessGraphVisitException("from_node reference {k!r} not found in process graph".format(k=key))
        return self._flat_graph[key]

    def _process_value(self, value) -> Any:
        if isinstance(value, dict):
            if "from_node" in value:
                key = value["from_node"]
                node = self._resolve_from_node(key=key)
                return self._process_from_node(key=key, node=node)
            elif "from_parameter" in value:
                name = value["from_parameter"]
                return self._process_from_parameter(name=name)
            elif "process_graph" in value:
                # Don't traverse child process graphs
                # TODO: should/can we? Can we know available parameters for validation, or do we skip validation?
                return value
            else:
                return {k: self._process_value(v) for (k, v) in value.items()}
        elif isinstance(value, (list, tuple)):
            return [self._process_value(v) for v in value]
        else:
            return value

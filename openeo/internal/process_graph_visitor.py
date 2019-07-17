import warnings
from abc import  ABC
from typing import Dict


class ProcessGraphVisitor(ABC):
    """
    Hierarchical Visitor for process graphs, to allow different tools to traverse the graph.
    """

    def __init__(self):
        self.process_stack = []

    @classmethod
    def _list_to_graph(cls,processGraph):
        # TODO remove this function when not used anymore in openeo-python-driver
        warnings.warn("_list_to_graph is deprecated, use dereference_node_arguments instead.", DeprecationWarning)
        return cls.dereference_from_node_arguments(processGraph)

    @classmethod
    def dereference_from_node_arguments(cls, process_graph:dict) -> str:
        """
        Walk through the given process graph and replace (in-place) "from_node" references in
        process arguments (dictionaries or lists) with the corresponding resolved subgraphs

        :param process_graph: process graph dictionary to be manipulated in-place
        :return: name of the "result" node of the graph
        """
        # TODO avoid manipulating process graph in place? make it more explicit? work on a copy? Where is this functionality used anyway?

        def resolve_from_node(process_graph, node, from_node):
            if from_node not in process_graph:
                raise ValueError('from_node {f!r} (referenced by {n!r}) not in process graph.'.format(
                    f=from_node, n=node))
            return process_graph[from_node]

        result_node = None
        for node, node_dict in process_graph.items():
            if node_dict.get("result", False):
                assert result_node is None
                result_node = node
            arguments = node_dict.get("arguments", {})
            for arg in arguments.values():
                if isinstance(arg, dict) and "from_node" in arg:
                    arg["node"] = resolve_from_node(process_graph, node, arg["from_node"])
                elif isinstance(arg, list):
                    for i, element in enumerate(arg):
                        if isinstance(element, dict) and "from_node" in element:
                            arg[i] = resolve_from_node(process_graph, node, element['from_node'])

        if result_node is None:
            raise ValueError("The provided process graph does not contain a result node.")
        return result_node


    def accept_process_graph(self,graph:Dict):
        """
        Traverse a process graph, provided as a flat Dict of nodes that are not referencing each other.
        :param graph:
        :return:
        """
        top_level_node = self.dereference_from_node_arguments(graph)
        self.accept(graph[top_level_node])
        return self

    def accept(self, node:Dict):
        if 'process_id' in node:
            pid = node['process_id']
            arguments = node.get('arguments',{})
            self.process_stack.append(pid)
            self.enterProcess(pid, arguments)
            for arg in arguments:
                value = arguments[arg]
                if type(value) == list:
                    self.enterArray(arg)
                    for array_element in value:
                        if type(array_element) is dict:
                            self.accept(array_element)
                            self.arrayElementDone()
                        else:
                            self.constantArrayElement(array_element)
                    self.leaveArray(arg)
                elif type(value) == dict:
                    self.enterArgument(arg,value)
                    if 'node' in value and 'from_node' in value:
                        self.accept(value['node'])
                    else:
                        self.accept(value)
                    self.leaveArgument(arg,value)
                else:
                    self.constantArgument(arg,value)

            self.leaveProcess(pid, arguments)
            self.process_stack.pop()

    def enterProcess(self,process_id, arguments:Dict):
        pass

    def leaveProcess(self, process_id, arguments: Dict):
        pass

    def enterArgument(self,argument_id,node:Dict):
        pass

    def leaveArgument(self, argument_id, node: Dict):
        pass

    def constantArgument(self,argument_id:str,value):
        pass

    def enterArray(self, argument_id):
        pass

    def constantArrayElement(self,value):
        pass

    def arrayElementDone(self):
        pass

    def leaveArray(self, argument_id):
        pass

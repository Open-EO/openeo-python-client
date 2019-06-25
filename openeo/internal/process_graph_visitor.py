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
        """
        Converts a list of process graph nodes into an actual graph, by resolving the references.
        :param processGraph:
        :return: a list containing the top level nodes in the DAG
        """
        result_node = None
        for node in processGraph:
            node_dict = processGraph.get(node)
            if (node_dict.get("result", False)):
                result_node = node
            arguments = node_dict.get("arguments", {})
            for a in arguments:
                arg = arguments[a]
                if type(arg) is dict and "from_node" in arg:
                    from_node_id = arg["from_node"]
                    from_node = processGraph.get(from_node_id, None)
                    if (from_node is None):
                        raise ValueError(
                            "Node not found in process graph: " + from_node_id + ". Referenced by: " + node)
                    arg["node"] = from_node
                elif type(arg) is list:
                    for num, element in enumerate(arg):
                        if type(element) == dict and "from_node" in element:
                            from_node_id = element["from_node"]
                            from_node = processGraph.get(from_node_id, None)
                            if (from_node is None):
                                raise ValueError(
                                    "Node not found in process graph: " + from_node_id + ". Referenced by: " + node)
                            arg[num] = from_node

        if result_node is None:
            raise ValueError("The provided process graph does not contain a result node.")
        return result_node

    def accept_process_graph(self,graph:Dict):
        """
        Traverse a process graph, provided as a flat Dict of nodes that are not referencing each other.
        :param graph:
        :return:
        """
        top_level_node = ProcessGraphVisitor._list_to_graph(graph)
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

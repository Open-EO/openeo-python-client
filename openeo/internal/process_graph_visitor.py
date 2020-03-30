from abc import ABC


class ProcessGraphVisitor(ABC):
    """
    Hierarchical Visitor for process graphs, to allow different tools to traverse the graph.
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

        # TODO avoid manipulating process graph in place? make it more explicit? work on a copy? Where is this functionality used anyway?
        # TODO this is driver specific functionality, working on flattened graph structures. Make this more clear?
        # TODO call it more something like "unflatten"?. Split this off of ProcessGraphVisitor?

        def resolve_from_node(process_graph, node, from_node):
            if from_node not in process_graph:
                raise ValueError('from_node {f!r} (referenced by {n!r}) not in process graph.'.format(
                    f=from_node, n=node))
            return process_graph[from_node]

        result_node = None
        for node, node_dict in process_graph.items():
            if node_dict.get("result", False):
                if result_node:
                    raise ValueError("Multiple result nodes: {a}, {b}".format(a=result_node, b=node))
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

    def accept_process_graph(self, graph: dict) -> 'ProcessGraphVisitor':
        """
        Traverse a (flat) process graph

        :param graph:
        :return:
        """
        # TODO: this is driver specific functionality, working on flattened graph structures. Make this more clear?
        top_level_node = self.dereference_from_node_arguments(graph)
        self.accept(graph[top_level_node])
        return self

    def accept(self, node: dict):
        self.accept_node(node)

    def accept_node(self, node: dict):
        pid = node['process_id']
        arguments = node.get('arguments', {})
        self._accept_process(process_id=pid, arguments=arguments)

    def _accept_process(self, process_id: str, arguments: dict):
        self.process_stack.append(process_id)
        self.enterProcess(process_id=process_id, arguments=arguments)
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
        self.leaveProcess(process_id=process_id, arguments=arguments)
        assert self.process_stack.pop() == process_id

    def _accept_argument_list(self, elements: list):
        for element in elements:
            if isinstance(element, dict):
                self._accept_argument_dict(element)
                self.arrayElementDone(element)
            else:
                self.constantArrayElement(element)

    def _accept_argument_dict(self, value: dict):
        if 'node' in value and 'from_node' in value:
            # TODO: this looks bit weird (or at least very specific).
            self.accept_node(value['node'])
        elif value.get("from_node"):
            self.accept_node(value['from_node'])
        elif "process_id" in value:
            self.accept_node(value)
        else:
            self._accept_dict(value)

    def _accept_dict(self, value: dict):
        pass

    def enterProcess(self, process_id: str, arguments: dict):
        pass

    def leaveProcess(self, process_id: str, arguments: dict):
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

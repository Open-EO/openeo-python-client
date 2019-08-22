import copy
from typing import Dict, Union


class GraphBuilder():

    def __init__(self, graph = None):
        """
            Create a process graph builder.
            If a graph is provided, its nodes will be added to this builder, this does not necessarily preserve id's of the nodes.

            :param graph: Dict : Optional, existing process graph
        """
        self.processes = {}
        self.id_counter = {}
        if graph is not None:
            self._merge_processes(graph)

    def copy(self):
        return GraphBuilder(self.processes)

    def add_process(self,process_id,result=None, **args):
        process_id = self.process(process_id, args)
        if result is not None:
            self.processes[process_id]["result"] = result
        return process_id

    def process(self,process_id, args):
        """
        Add a process and return the id. Do not add a  new process if it already exists in the graph.

        :param process_id:
        :param args:
        :return:
        """
        new_process = {
            'process_id': process_id,
            'arguments': args,
            'result': False
        }
        #try:
        #    existing_id = list(self.processes.keys())[list(self.processes.values()).index(new_process)]
        #    return existing_id
        #except ValueError as e:
        #    pass
        id = self._generate_id(process_id)
        self.processes[id] = new_process
        return id

    def _generate_id(self,name:str):
        name = name.replace("_","")
        if( not self.id_counter.get(name)):
            self.id_counter[name] = 1
        else:
            self.id_counter[name] += 1
        return name + str(self.id_counter[name])

    def merge(self, other: 'GraphBuilder'):
        return GraphBuilder(self.processes)._merge_processes(other.processes)

    def _merge_processes(self, processes: Dict, return_key_map=False):
        # Maps original node key to new key in merged result
        key_map = {}
        node_refs = []
        for key,process in sorted(processes.items()):
            process_id = process['process_id']
            args = process['arguments']
            result = process.get('result', None)
            args_copy = copy.deepcopy(args)
            id = self.process(process_id, args_copy)
            if id != key:
                key_map[key] = id
            node_refs += self._extract_node_references(args_copy)

            if result is not None:
                self.processes[id]['result'] = result

        for node_ref in node_refs:
            old_node_id = node_ref['from_node']
            if old_node_id in key_map:
                node_ref['from_node'] = key_map[old_node_id]

        if return_key_map:
            return self, key_map
        else:
            return self

    def _extract_node_references(self, arguments):
        node_ref_list = []
        for argument in arguments.values():
            if isinstance(argument, dict):
                if 'from_node' in argument:
                    node_ref_list.append(argument)
            if isinstance(argument,list):
                for element in argument:
                    if isinstance(element, dict):
                        if 'from_node' in element:
                            node_ref_list.append(element)
        return node_ref_list

    def find_result_node_id(self):
        result_node_ids = [k for k,v in self.processes.items() if v.get('result',False)]
        if len(result_node_ids) == 1:
            return result_node_ids[0]
        else:
            raise RuntimeError("Invalid list of result node id's: " + str(result_node_ids))

    @classmethod
    def combine(cls, operator: str, first: Union['GraphBuilder', dict], second: Union['GraphBuilder', dict], arg_name='data'):
        """Combine two GraphBuilders to a new merged one using the given operator"""
        merged = cls()

        def insert_builder(builder: GraphBuilder):
            nonlocal merged
            result_node = builder.find_result_node_id()
            _, key_map = merged._merge_processes(builder.processes, return_key_map=True)
            key = key_map.get(result_node, result_node)
            merged.processes[key]['result'] = False
            return {'from_node': key}

        if isinstance(first, GraphBuilder):
            first = insert_builder(first)
        assert isinstance(first, dict)
        if isinstance(second, GraphBuilder):
            second = insert_builder(second)
        assert isinstance(second, dict)

        args = {
            arg_name:[first, second]
        }

        merged.add_process(operator, result=True, **args)
        return merged

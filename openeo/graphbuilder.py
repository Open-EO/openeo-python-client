from typing import Dict

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

    def add_process(self,process_id,result=None, **args):
        process_id = self.process(process_id, args)
        if result != None:
            self.processes[process_id]["result"] = result
        return process_id

    def process(self,process_id, args):
        id = self._generate_id(process_id)
        self.processes[id] = {
            'process_id': process_id,
            'arguments': args
        }
        return id

    def _generate_id(self,name:str):
        name = name.replace("_","")
        if( not self.id_counter.get(name)):
            self.id_counter[name] = 1
        else:
            self.id_counter[name] += 1
        return name + str(self.id_counter[name])


    def merge(self, other:'GraphBuilder'):
        return self._merge_processes(other.processes)

    def _merge_processes(self, processes:Dict):
        for process in processes.values():
            process_id = process['process_id']
            args = process['arguments']
            result = process.get('result', None)
            id = self.process(process_id, args)
            if result != None:
                self.processes[id]['result'] = result
        return self

    def find_result_node_id(self):
        result_node_ids = [k for k,v in self.processes.items() if v.get('result',False)]
        if len(result_node_ids) == 1:
            return result_node_ids[0]
        else:
            raise RuntimeError("Invalid list of result node id's: " + str(result_node_ids))


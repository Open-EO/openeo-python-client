
class GraphBuilder():



    def __init__(self):
        self.processes = {}
        self.id_counter = {}

    def process(self,process_id, args= {}):
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

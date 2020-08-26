
from openeo import ImageCollection
from openeo.internal.graph_building import PGNode


class ProcessBuilder(ImageCollection):
    """
    An object to construct process graphs for callbacks.
    """

    PARENT='PARENT'

    @classmethod
    def datacube_callback(cls,datacube:'DataCube'):
        def callback(pgnode,process_argname='process'):
            parent_node: PGNode = datacube.processgraph_node
            parent_node.arguments[process_argname] = {'process_graph':pgnode}
            return datacube
        return callback

    def __init__(self, final_callback=None, pgnode=None, parent_data_parameter='data'):
        """

        @param final_callback: A function to invoke when the graph is ready.
        @param pgnode:
        @param parent_data_parameter:
        """
        self.final_callback = final_callback
        self.pgnode = pgnode
        self.parent_data_parameter = parent_data_parameter

    def _ancestor(self):
        if self.pgnode is not None:
            return self.pgnode
        else:
            return {'from_parameter': self.parent_data_parameter}

    def run_udf(self,code=str,runtime:str=""):
        return self.process('run_udf',{'data':self._ancestor(),'udf':code,'runtime':runtime})

    def absolute(self):
        return self.process('absolute', {'x': self._ancestor()})

    def process(self, process_id: str, arguments: dict = None, **kwargs) -> 'ProcessBuilder':
        """
        Generic helper to create a new DataCube by applying a process.

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :return: new DataCube instance
        """
        arguments = {**(arguments or {}), **kwargs}
        return ProcessBuilder(self.final_callback, PGNode(process_id=process_id, arguments=arguments))

    def done(self):
        return self.finish()

    def finish(self):
        return self.final_callback(self.pgnode)
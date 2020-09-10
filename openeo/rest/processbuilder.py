
from openeo import ImageCollection
from openeo.internal.graph_building import PGNode

def max(data:'ProcessBuilder',ignore_nodata=True) -> 'ProcessBuilder':
    """
    Computes the largest value of an array of numbers, which is is equal to the first element of a sorted (i.e., ordered) version the array.

    An array without non-null elements resolves always with null.

    @param ignore_nodata:
    @return:
    """
    return data.max(ignore_nodata)

def array_element(data,index=None,label=None,return_nodata=None):
    args = {'data': data._ancestor()}
    if index is not None:
        args['index']=index
    elif label is not None:
        args['label'] = label
    else:
        raise ValueError("Either the index or label argument should be specified.")

    if return_nodata is not None:
        args['return_nodata'] = return_nodata
    return data.process('array_element', args)


def add(x:'ProcessBuilder',y:'ProcessBuilder') -> 'ProcessBuilder':

    args = {
        'x': x._ancestor(),
        'y': y._ancestor()
            }
    return x.process('add', args)


class ProcessBuilder(ImageCollection):
    """
    An object to construct process graphs for callbacks.
    """

    PARENT='PARENT'

    def __init__(self, pgnode=None, parent_data_parameter='data'):
        """

        @param pgnode:
        @param parent_data_parameter:
        """
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

    def __add__(self, other) -> 'ProcessBuilder':
        return add(self,other)

    def max(self,ignore_nodata=True):
        """
        Computes the largest value of an array of numbers, which is is equal to the first element of a sorted (i.e., ordered) version the array.

        An array without non-null elements resolves always with null.

        @param ignore_nodata:
        @return:
        """
        return self.process('max', {'data': self._ancestor(), 'ignore_nodata':ignore_nodata})

    def process(self, process_id: str, arguments: dict = None, **kwargs) -> 'ProcessBuilder':
        """
        Generic helper to create a new DataCube by applying a process.

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :return: new DataCube instance
        """
        arguments = {**(arguments or {}), **kwargs}
        return ProcessBuilder(PGNode(process_id=process_id, arguments=arguments))



def absolute(data: ProcessBuilder) -> ProcessBuilder:
    return data.absolute()

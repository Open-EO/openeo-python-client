from typing import Union

from openeo.internal.graph_building import PGNode

UNSET = object()


class ProcessBuilderBase:
    """
    Base implementation of a builder pattern that allows constructing process graphs
    by calling functions.
    """

    # TODO: can this implementation be merged with PGNode directly?

    def __init__(self, pgnode: Union[PGNode, dict, list]):
        self.pgnode = pgnode

    @classmethod
    def process(cls, process_id: str, arguments: dict = None, namespace: Union[str, None] = None, **kwargs):
        """
        Apply process, using given arguments

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :param namespace: process namespace (only necessary to specify for non-predefined or non-user-defined processes)
        :return: new ProcessBuilder instance
        """
        arguments = {**(arguments or {}), **kwargs}
        for arg, value in arguments.items():
            if isinstance(value, ProcessBuilderBase):
                arguments[arg] = value.pgnode
            elif isinstance(value,list):
                for index,arrayelement in enumerate(value):
                    if(isinstance(arrayelement,ProcessBuilderBase)):
                        value[index] = arrayelement.pgnode

        for arg in [a for a, v in arguments.items() if v is UNSET]:
            del arguments[arg]
        return cls(PGNode(process_id=process_id, arguments=arguments, namespace=namespace))

    def flat_graph(self) -> dict:
        """Get the process graph in flat dict representation"""
        return self.pgnode.flat_graph()

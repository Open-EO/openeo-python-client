from ..udp import ProcessGraph
import typing

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection


class RESTProcessGraph(ProcessGraph):
    def __init__(self, process_graph_id: str, connection: 'Connection'):
        super().__init__(process_graph_id)
        self._connection = connection

    def update(self, process_graph: dict, **kwargs) -> ProcessGraph:
        return self._connection.save_process_graph(self.process_graph_id, process_graph, **kwargs)

    def describe(self) -> dict:
        return self._connection.get(path="/process_graphs/{}".format(self.process_graph_id)).json()

    def delete(self) -> None:
        raise NotImplementedError

    def validate(self) -> None:
        raise NotImplementedError

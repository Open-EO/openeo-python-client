from ..udp import ProcessGraph
import typing

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection


class RESTProcessGraph(ProcessGraph):
    def __init__(self, user_defined_process_id: str, connection: 'Connection'):
        super().__init__(user_defined_process_id)
        self._connection = connection

    def update(self, process_graph: dict, **metadata) -> ProcessGraph:
        return self._connection.save_user_defined_process(self.user_defined_process_id, process_graph, **metadata)

    def describe(self) -> dict:
        return self._connection.get(path="/process_graphs/{}".format(self.user_defined_process_id)).json()

    def delete(self) -> None:
        raise NotImplementedError

    def validate(self) -> None:
        raise NotImplementedError

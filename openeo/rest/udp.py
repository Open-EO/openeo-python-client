import typing
from typing import List
from collections import namedtuple

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection


Parameter = namedtuple('Parameter', ['name', 'description', 'schema'])


class RESTUserDefinedProcess:
    def __init__(self, user_defined_process_id: str, connection: 'Connection'):
        self.user_defined_process_id = user_defined_process_id
        self._connection = connection

    def update(self, process_graph: dict, parameters: List[Parameter] = None) -> 'RESTUserDefinedProcess':
        return self._connection.save_user_defined_process(self.user_defined_process_id, process_graph, parameters)

    def describe(self) -> dict:
        return self._connection.get(path="/process_graphs/{}".format(self.user_defined_process_id)).json()

    def delete(self) -> None:
        self._connection.delete(path="/process_graphs/{}".format(self.user_defined_process_id))

    def validate(self) -> None:
        raise NotImplementedError

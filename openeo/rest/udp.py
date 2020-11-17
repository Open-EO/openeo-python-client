import typing
from typing import List, Union

from openeo.api.process import Parameter

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection


class RESTUserDefinedProcess:
    def __init__(self, user_defined_process_id: str, connection: 'Connection'):
        self.user_defined_process_id = user_defined_process_id
        self._connection = connection

    def store(self, process_graph: dict, parameters: List[Union[Parameter, dict]] = None, public: bool = False, summary:str = None, description:str=None):
        req = {
            'process_graph': process_graph,
            'public': public
        }
        if parameters is not None:
            req["parameters"] = [
                (p if isinstance(p, Parameter) else Parameter(**p)).to_dict()
                for p in parameters
            ]
        if summary is not None:
            req["summary"] = summary
        if description is not None:
            req["description"] = description
        self._connection.put(path="/process_graphs/{}".format(self.user_defined_process_id), json=req)

    def update(self, process_graph: dict, parameters: List[Union[Parameter, dict]] = None, public: bool = False, summary:str = None, description:str=None):
        self.store(process_graph=process_graph, parameters=parameters, public=public, summary=summary,description=description)

    def describe(self) -> dict:
        # TODO: parse the "parameters" to Parameter objects?
        return self._connection.get(path="/process_graphs/{}".format(self.user_defined_process_id)).json()

    def delete(self) -> None:
        self._connection.delete(path="/process_graphs/{}".format(self.user_defined_process_id), expected_status=204)

    def validate(self) -> None:
        raise NotImplementedError

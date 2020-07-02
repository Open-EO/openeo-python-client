import typing
from typing import List, Union

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection


class Parameter:
    """Process parameter"""
    _DEFAULT_UNDEFINED = object()

    def __init__(self, name: str, description: str, schema: Union[dict, str], default=_DEFAULT_UNDEFINED):
        self.name = name
        self.description = description
        self.schema = {"type": schema} if isinstance(schema, str) else schema
        self.default = default

    @classmethod
    def raster_cube(cls, name: str = "data", description: str = "A data cube."):
        """Helper to easily create a 'raster-cube' parameter."""
        return cls(name=name, description=description, schema={"type": "object", "subtype": "raster-cube"})

    def to_dict(self) -> dict:
        """Convert to dict for JSON-serialization."""
        d = {"name": self.name, "description": self.description, "schema": self.schema}
        if self.default is not self._DEFAULT_UNDEFINED:
            d["default"] = self.default
        return d


class RESTUserDefinedProcess:
    def __init__(self, user_defined_process_id: str, connection: 'Connection'):
        self.user_defined_process_id = user_defined_process_id
        self._connection = connection

    def store(self, process_graph: dict, parameters: List[Union[Parameter, dict]] = None, public: bool = False):
        req = {
            'process_graph': process_graph,
            'public': public
        }
        if parameters is not None:
            req["parameters"] = [
                (p if isinstance(p, Parameter) else Parameter(**p)).to_dict()
                for p in parameters
            ]
        self._connection.put(path="/process_graphs/{}".format(self.user_defined_process_id), json=req)

    def update(self, process_graph: dict, parameters: List[Union[Parameter, dict]] = None, public: bool = False):
        self.store(process_graph=process_graph, parameters=parameters, public=public)

    def describe(self) -> dict:
        # TODO: parse the "parameters" to Parameter objects?
        return self._connection.get(path="/process_graphs/{}".format(self.user_defined_process_id)).json()

    def delete(self) -> None:
        self._connection.delete(path="/process_graphs/{}".format(self.user_defined_process_id), expected_status=204)

    def validate(self) -> None:
        raise NotImplementedError

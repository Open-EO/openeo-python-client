import warnings
from typing import Union


class Parameter:
    """
    Wrapper for a process parameter, as used in predefined and user-defined processes.
    """
    # TODO unify with openeo.internal.processes.parse.Parameter?

    _DEFAULT_UNDEFINED = object()

    def __init__(
            self, name: str, description: str = None, schema: Union[dict, str] = None,
            default=_DEFAULT_UNDEFINED, optional=None
    ):
        self.name = name
        if description is None:
            # Description is required in openEO API, we are a bit more permissive here.
            warnings.warn("Parameter without description: using name as description.")
            description = name
        self.description = description
        self.schema = {"type": schema} if isinstance(schema, str) else (schema or {})
        self.default = default
        self.optional = optional

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON-serialization."""
        d = {"name": self.name, "description": self.description, "schema": self.schema}
        if self.optional is not None:
            d["optional"] = self.optional
        if self.default is not self._DEFAULT_UNDEFINED:
            d["default"] = self.default
            d["optional"] = True
        return d

    @classmethod
    def raster_cube(cls, name: str = "data", description: str = "A data cube.") -> 'Parameter':
        """
        Helper to easily create a 'raster-cube' parameter.

        :param name: name of the parameter.
        :param description: description of the parameter
        :return: Parameter
        """
        return cls(name=name, description=description, schema={"type": "object", "subtype": "raster-cube"})

    @classmethod
    def string(cls, name: str, description: str = None, default=_DEFAULT_UNDEFINED, values=None) -> 'Parameter':
        """Helper to create a 'string' type parameter."""
        schema = {"type": "string"}
        if values is not None:
            schema["enum"] = values
        return cls(name=name, description=description, schema=schema, default=default)


    @classmethod
    def integer(cls, name: str, description: str = None, default=_DEFAULT_UNDEFINED) -> 'Parameter':
        """Helper to create a 'integer' type parameter."""
        return cls(name=name, description=description, schema={"type": "integer"}, default=default)

    @classmethod
    def number(cls, name: str, description: str = None, default=_DEFAULT_UNDEFINED) -> 'Parameter':
        """Helper to create a 'number' type parameter."""
        return cls(name=name, description=description, schema={"type": "number"}, default=default)

    @classmethod
    def boolean(cls, name: str, description: str = None, default=_DEFAULT_UNDEFINED) -> 'Parameter':
        """Helper to create a 'boolean' type parameter."""
        return cls(name=name, description=description, schema={"type": "boolean"}, default=default)

    @classmethod
    def array(cls, name: str, description: str = None, default=_DEFAULT_UNDEFINED) -> 'Parameter':
        """Helper to create a 'array' type parameter."""
        return cls(name=name, description=description, schema={"type": "array"}, default=default)

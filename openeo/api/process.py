from typing import Union


class Parameter:
    """
    Wrapper for a process parameter, as used in predefined and user-defined processes.
    """
    _DEFAULT_UNDEFINED = object()

    def __init__(self, name: str, description: str, schema: Union[dict, str], default=_DEFAULT_UNDEFINED):
        self.name = name
        self.description = description
        self.schema = {"type": schema} if isinstance(schema, str) else schema
        self.default = default

    @classmethod
    def raster_cube(cls, name: str = "data", description: str = "A data cube."):
        """
        Helper to easily create a 'raster-cube' parameter.

        :param name: name of the parameter.
        :param description: description of the parameter
        :return: Parameter
        """
        return cls(name=name, description=description, schema={"type": "object", "subtype": "raster-cube"})

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON-serialization."""
        d = {"name": self.name, "description": self.description, "schema": self.schema}
        if self.default is not self._DEFAULT_UNDEFINED:
            d["default"] = self.default
            d["optional"] = True
        return d

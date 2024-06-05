from __future__ import annotations

import warnings
from typing import List, Optional, Union


class Parameter:
    """
    A (process) parameter to build parameterized
    :ref:`user-defined processes<user-defined-processes>`.

    Parameter objects can be :ref:`defined <udp-declaring-parameters>`
    with at least a name and expected schema
    (e.g. is the parameter a placeholder for a string, a bounding box, a date, ...)
    and can then be :ref:`used <build_and_store_udp>`
    with various functions and classes,
    like :py:class:`~openeo.rest.datacube.DataCube`,
    to build parameterized user-defined processes.

    Apart from the generic :py:class:`Parameter` constructor,
    this class also provides various helpers (class methods)
    to easily create parameters for common parameter types.

    :param name: parameter name, which will be used to assign concrete values to.
        It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
    :param description: human-readable description of the parameter.
    :param schema: JSON schema describing the expected data type and structure of the parameter.
    :param default: default value for the parameter when it's optional.
    :param optional: toggle to indicate whether the parameter is optional or required.
    """
    # TODO unify with openeo.internal.processes.parse.Parameter?
    __slots__ = ("name", "description", "schema", "default", "optional")

    _DEFAULT_UNDEFINED = object()

    def __init__(
        self,
        name: str,
        description: Optional[str] = None,
        schema: Union[dict, str, None] = None,
        default=_DEFAULT_UNDEFINED,
        optional: Optional[bool] = None,
    ):
        self.name = name
        if description is None:
            # Description is required in openEO API, we are a bit more permissive here.
            warnings.warn("Parameter without description: using name as description.")
            description = name
        self.description = description
        self.schema = {"type": schema} if isinstance(schema, str) else (schema or {})
        # TODO: automatically set `optional` when `default` is set?
        self.default = default
        self.optional = optional

    def to_dict(self) -> dict:
        """
        Convert to dictionary for JSON-serialization.
        """
        d = {"name": self.name, "description": self.description, "schema": self.schema}
        if self.optional is not None:
            d["optional"] = self.optional
        if self.default is not self._DEFAULT_UNDEFINED:
            d["default"] = self.default
            d["optional"] = True
        return d

    @classmethod
    def raster_cube(cls, name: str = "data", description: str = "A data cube.", **kwargs) -> Parameter:
        """
        Helper to easily create a 'raster-cube' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).
        """
        schema = {"type": "object", "subtype": "raster-cube"}
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def datacube(cls, name: str = "data", description: str = "A data cube.", **kwargs) -> Parameter:
        """
        Helper to easily create a 'datacube' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionadded:: 0.22.0
        """
        schema = {"type": "object", "subtype": "datacube"}
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def string(
        cls,
        name: str,
        description: Optional[str] = None,
        *,
        values: Optional[List[str]] = None,
        subtype: Optional[str] = None,
        format: Optional[str] = None,
        **kwargs,
    ) -> Parameter:
        """
        Helper to easily create a 'string' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.
        :param values: Optional list of allowed string values to make this an "enum".
        :param subtype: Optional subtype of the 'string' schema.
        :param format: Optional format of the 'string' schema.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).
        """
        schema = {"type": "string"}
        if values is not None:
            schema["enum"] = values
        if subtype:
            schema["subtype"] = subtype
        if format:
            schema["format"] = format
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def integer(cls, name: str, description: Optional[str] = None, **kwargs) -> Parameter:
        """
        Helper to create an 'integer' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).
        """
        return cls(name=name, description=description, schema={"type": "integer"}, **kwargs)

    @classmethod
    def number(cls, name: str, description: Optional[str] = None, **kwargs) -> Parameter:
        """
        Helper to easily create a 'number' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).
        """
        return cls(name=name, description=description, schema={"type": "number"}, **kwargs)

    @classmethod
    def boolean(cls, name: str, description: Optional[str] = None, **kwargs) -> Parameter:
        """
        Helper to easily create a 'boolean' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).
        """
        return cls(name=name, description=description, schema={"type": "boolean"}, **kwargs)

    @classmethod
    def array(
        cls,
        name: str,
        description: Optional[str] = None,
        *,
        item_schema: Optional[Union[str, dict]] = None,
        **kwargs,
    ) -> Parameter:
        """
        Helper to easily create parameter with an 'array' schema.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.
        :param item_schema: Schema of the array items given in JSON Schema style, e.g. ``{"type": "string"}``.
            Simple schemas can also be specified as single string:
            e.g. ``"string"`` will be expanded to ``{"type": "string"}``.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionchanged:: 0.23.0
            Added ``item_schema`` argument.
        """
        schema = {"type": "array"}
        if item_schema:
            if isinstance(item_schema, str):
                item_schema = {"type": item_schema}
            schema["items"] = item_schema
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def object(
        cls, name: str, description: Optional[str] = None, *, subtype: Optional[str] = None, **kwargs
    ) -> Parameter:
        """
        Helper to create an 'object' type parameter

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.
        :param subtype: subtype of the 'object' schema

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionadded:: 0.26.0
        """
        schema = {"type": "object"}
        if subtype:
            schema["subtype"] = subtype
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def bounding_box(
        cls,
        name: str,
        description: str = "Spatial extent specified as a bounding box with 'west', 'south', 'east' and 'north' fields.",
        **kwargs,
    ) -> Parameter:
        """
        Helper to easily create a 'bounding box' parameter, which allows to specify a spatial extent
        with "west", "south", "east" and "north" bounds (and optionally a CRS identifier).

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionadded:: 0.30.0
        """
        schema = {
            "type": "object",
            "subtype": "bounding-box",
            "required": ["west", "south", "east", "north"],
            "properties": {
                "west": {
                    "type": "number",
                    "description": "West (lower left corner, coordinate axis 1).",
                },
                "south": {
                    "type": "number",
                    "description": "South (lower left corner, coordinate axis 2).",
                },
                "east": {
                    "type": "number",
                    "description": "East (upper right corner, coordinate axis 1).",
                },
                "north": {
                    "type": "number",
                    "description": "North (upper right corner, coordinate axis 2).",
                },
                "crs": {
                    "description": "Coordinate reference system of the extent, specified as as [EPSG code](http://www.epsg-registry.org/) or [WKT2 CRS string](http://docs.opengeospatial.org/is/18-010r7/18-010r7.html). Defaults to `4326` (EPSG code 4326) unless the client explicitly requests a different coordinate reference system.",
                    "anyOf": [
                        {
                            "type": "integer",
                            "subtype": "epsg-code",
                            "title": "EPSG Code",
                            "minimum": 1000,
                        },
                        {
                            "type": "string",
                            "subtype": "wkt2-definition",
                            "title": "WKT2 definition",
                        },
                    ],
                    "default": 4326,
                },
                # TODO: support base and height?
            },
        }
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def date(cls, name: str, description: str = "A date.", **kwargs) -> Parameter:
        """
        Helper to easily create a 'date' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionadded:: 0.30.0
        """
        schema = {"type": "string", "subtype": "date", "format": "date"}
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def date_time(cls, name: str, description: str = "A date with time.", **kwargs) -> Parameter:
        """
        Helper to easily create a 'date-time' parameter.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionadded:: 0.30.0
        """
        schema = {"type": "string", "subtype": "date-time", "format": "date-time"}
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def geojson(cls, name: str, description: str = "Geometries specified as GeoJSON object.", **kwargs) -> Parameter:
        """
        Helper to easily create a 'geojson' parameter, which allows to specify geometries as an inline GeoJSON object.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionadded:: 0.30.0
        """
        schema = {"type": "object", "subtype": "geojson"}
        return cls(name=name, description=description, schema=schema, **kwargs)

    @classmethod
    def temporal_interval(
        cls,
        name: str,
        description: str = "Temporal extent specified as two-element array with start and end date/date-time.",
        **kwargs,
    ) -> Parameter:
        """
        Helper to easily create a 'temporal-interval' parameter, which allows to specify a temporal extent
        as a two-element array with start and end date/date-time.

        :param name: parameter name, which will be used to assign concrete values to.
            It is recommended to stick to the convention of snake case naming (using lowercase with underscores).
        :param description: human-readable description of the parameter.

        See the generic :py:class:`Parameter` constructor for information on additional arguments (except ``schema``).

        .. versionadded:: 0.30.0
        """
        schema = {
            "type": "array",
            "subtype": "temporal-interval",
            "uniqueItems": True,
            "minItems": 2,
            "maxItems": 2,
            "items": {
                "anyOf": [
                    {"type": "string", "subtype": "date-time", "format": "date-time"},
                    {"type": "string", "subtype": "date", "format": "date"},
                    {"type": "null"},
                ]
            },
        }
        return cls(name=name, description=description, schema=schema, **kwargs)

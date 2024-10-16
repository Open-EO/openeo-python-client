"""
Functionality and tools to process openEO processes.
For example: parse a bunch of JSON descriptions and generate Python (stub) functions.
"""

from __future__ import annotations

import json
import re
import typing
from pathlib import Path
from typing import Any, Iterator, List, Optional, Union

import requests


class Schema(typing.NamedTuple):
    """Schema description of an openEO process parameter or return value."""

    schema: Union[dict, list]

    @classmethod
    def from_dict(cls, data: dict) -> Schema:
        return cls(schema=data)

    def is_process_graph(self) -> bool:
        """Is this a  {"type": "object", "subtype": "process-graph"} schema?"""
        return (
            isinstance(self.schema, dict)
            and self.schema.get("type") == "object"
            and self.schema.get("subtype") == "process-graph"
        )

    def accepts_geojson(self) -> bool:
        """Does this schema accept inline GeoJSON objects?"""

        def is_geojson_schema(schema) -> bool:
            return isinstance(schema, dict) and schema.get("type") == "object" and schema.get("subtype") == "geojson"

        if isinstance(self.schema, dict):
            return is_geojson_schema(self.schema)
        elif isinstance(self.schema, list):
            return any(is_geojson_schema(s) for s in self.schema)
        return False


_NO_DEFAULT = object()


class Parameter(typing.NamedTuple):
    """openEO process parameter"""
    # TODO unify with openeo.api.process.Parameter?

    name: str
    description: str
    schema: Schema
    default: Any = _NO_DEFAULT
    optional: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> Parameter:
        return cls(
            name=data["name"],
            description=data["description"],
            schema=Schema.from_dict(data["schema"]),
            default=data.get("default", _NO_DEFAULT),
            optional=data.get("optional", False),
        )

    def has_default(self):
        return self.default is not _NO_DEFAULT


class Returns:
    """openEO process return description."""

    def __init__(self, description: str, schema: Schema):
        self.description = description
        self.schema = schema

    @classmethod
    def from_dict(cls, data: dict) -> Returns:
        return cls(description=data["description"], schema=Schema.from_dict(data["schema"]))


class Process(typing.NamedTuple):
    """
    Container for a opneEO process definition of an openEO process,
    covering pre-defined processes, user-defined processes,
    remote process definitions, etc.
    """

    # Common-denominator-wise only the process id is a required field in a process definition.
    # Depending on the context in the openEO API, some other fields (e.g. "process_graph")
    # may also be required.
    id: str
    parameters: Optional[List[Parameter]] = None
    returns: Optional[Returns] = None
    description: Optional[str] = None
    summary: Optional[str] = None
    # TODO: more properties?

    @classmethod
    def from_dict(cls, data: dict) -> Process:
        """Construct openEO process from dictionary values"""
        return cls(
            id=data["id"],
            parameters=[Parameter.from_dict(d) for d in data["parameters"]] if "parameters" in data else None,
            returns=Returns.from_dict(data["returns"]) if "returns" in data else None,
            description=data.get("description"),
            summary=data.get("summary"),
        )

    @classmethod
    def from_json(cls, data: str) -> Process:
        """Parse openEO process JSON description."""
        return cls.from_dict(json.loads(data))

    @classmethod
    def from_json_url(cls, url: str) -> Process:
        """Parse openEO process JSON description from given URL."""
        return cls.from_dict(requests.get(url).json())

    @classmethod
    def from_json_file(cls, path: Union[str, Path]) -> Process:
        """Parse openEO process JSON description file."""
        with Path(path).open("r") as f:
            return cls.from_json(f.read())


def parse_all_from_dir(path: Union[str, Path], pattern="*.json") -> Iterator[Process]:
    """Parse all openEO process files in given directory"""
    for p in sorted(Path(path).glob(pattern)):
        yield Process.from_json_file(p)


def parse_remote_process_definition(namespace: str, process_id: Optional[str] = None) -> Process:
    """
    Parse a process definition as defined by the "Remote Process Definition Extension" spec
    https://github.com/Open-EO/openeo-api/tree/draft/extensions/remote-process-definition
    """
    if not re.match("https?://", namespace):
        raise ValueError(f"Expected absolute URL, but got {namespace!r}")

    resp = requests.get(url=namespace)
    resp.raise_for_status()
    data = resp.json()
    assert isinstance(data, dict)

    if "id" not in data and "processes" in data and isinstance(data["processes"], list):
        # Handle process listing: filter out right process
        if not isinstance(process_id, str):
            raise ValueError(f"Working with process listing, but got invalid process id {process_id!r}")
        processes = [p for p in data["processes"] if p.get("id") == process_id]
        if len(processes) != 1:
            raise LookupError(f"Process {process_id!r} not found in process listing {namespace!r}")
        (data,) = processes

    # Some final validation.
    assert "id" in data, "Process definition should at least have an 'id' field"
    if process_id is not None and data["id"] != process_id:
        raise LookupError(f"Expected process id {process_id!r}, but found {data['id']!r}")

    return Process.from_dict(data)

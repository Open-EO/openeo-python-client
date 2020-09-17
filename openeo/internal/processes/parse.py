"""
Functionality and tools to process openEO processes.
For example: parse a bunch of JSON descriptions and generate Python (stub) functions.
"""
import json
from pathlib import Path
from typing import List, Union, Iterator

import requests


class Schema:
    """Schema description of an openEO process parameter or return value."""

    def __init__(self, schema: Union[dict, list]):
        self.schema = schema

    @classmethod
    def from_dict(cls, data: dict) -> 'Schema':
        return cls(schema=data)


class Parameter:
    """openEO process parameter"""

    NO_DEFAULT = object()

    def __init__(self, name: str, description: str, schema: Schema, default=NO_DEFAULT, optional: bool = False):
        self.name = name
        self.description = description
        self.schema = schema
        self.default = default
        self.optional = optional

    @classmethod
    def from_dict(cls, data: dict) -> 'Parameter':
        return cls(
            name=data["name"], description=data["description"], schema=Schema.from_dict(data["schema"]),
            default=data.get("default", cls.NO_DEFAULT), optional=data.get("optional", False)
        )

    def has_default(self):
        return self.default is not self.NO_DEFAULT


class Returns:
    """openEO process return description."""

    def __init__(self, description: str, schema: Schema):
        self.description = description
        self.schema = schema

    @classmethod
    def from_dict(cls, data: dict) -> 'Returns':
        return cls(description=data["description"], schema=Schema.from_dict(data["schema"]))


class Process:
    """An openEO process"""

    def __init__(
            self, id: str, parameters: List[Parameter], returns: Returns,
            description: str = "", summary: str = ""
    ):
        self.id = id
        self.description = description
        self.parameters = parameters
        self.returns = returns
        self.summary = summary
        # TODO: more properties?

    @classmethod
    def from_dict(cls, data: dict) -> 'Process':
        """Construct openEO process from dictionary values"""
        return cls(
            id=data["id"],
            parameters=[Parameter.from_dict(d) for d in data["parameters"]],
            returns=Returns.from_dict(data["returns"]),
            description=data["description"],
            summary=data["summary"],
        )

    @classmethod
    def from_json(cls, data: str) -> 'Process':
        """Parse openEO process JSON description."""
        return cls.from_dict(json.loads(data))

    @classmethod
    def from_json_url(cls, url: str) -> 'Process':
        """Parse openEO process JSON description from given URL."""
        return cls.from_dict(requests.get(url).json())

    @classmethod
    def from_json_file(cls, path: Union[str, Path]) -> 'Process':
        """Parse openEO process JSON description file."""
        with Path(path).open("r") as f:
            return cls.from_json(f.read())


def parse_all_from_dir(path: Union[str, Path], pattern="*.json") -> Iterator[Process]:
    """Parse all openEO process files in given directory"""
    for p in sorted(Path(path).glob(pattern)):
        yield Process.from_json_file(p)

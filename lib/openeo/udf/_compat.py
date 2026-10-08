import json
import re
from typing import Union

# TODO #465 move this to a more general utility subpackage?

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        # Will be assigned with fallback implementation below
        tomllib = None


class FlimsyTomlParser:
    """
    This is a rudimentary, low-tech, incomplete implementation of TOML parsing functionality
    for simple TOML use cases where the dependency on a full-fledged TOML library is not justified.
    For these simple uses cases, it should act as a best-effort drop-in replacement
    for the `loads()` functionality from full-fledged TOML libraries
    like `tomllib` (part of standard library since Python 3.11)
    or `tomli` (`tomllib` backport for earlier Python versions).
    """

    class TomlParseError(ValueError):
        pass

    KEY_PAIR_REGEX = re.compile(
        r"(?P<key>^[a-z0-9_-]+)\s*=\s*(?P<value>.*(\s+^\s+.*)*(\s+^])?)",
        flags=re.MULTILINE | re.VERBOSE | re.IGNORECASE,
    )

    @classmethod
    def loads(cls, data: str) -> dict:
        if re.search(r"^\[", data, flags=re.MULTILINE):
            raise cls.TomlParseError("Tables are not supported")
        if re.search(r"^[a-z0-9_-]+\.[a-z0-9_.-]+\s*=", data, flags=re.MULTILINE | re.IGNORECASE):
            raise cls.TomlParseError("Dotted keys are not supported")
        return {
            match.group("key"): cls._parse_toml_value_like_json(match.group("value"))
            for match in cls.KEY_PAIR_REGEX.finditer(data)
        }

    @classmethod
    def _parse_toml_value_like_json(cls, value: str) -> Union[int, float, list]:
        """
        Try to parse a TOML value by pretending it's (almost) JSON,
        which covers the basics (simple strings, numbers, arrays, a bit of nesting, ...)
        """
        # A bit of preprocessing to make it more JSON-like (strip comments, strip trailing commas)
        value = re.sub(r"#.*$", "", value, flags=re.MULTILINE)
        value = re.sub(r",\s*\]", "]", value)
        # Rudimentarily convert single quote strings to double quotes.
        value = re.sub("'([^'\"]*)'", r'"\1"', value)
        try:
            data = json.loads(value)
        except json.JSONDecodeError as e:
            raise cls.TomlParseError(f"Failed to parse TOML value {value!r}") from e
        return data


if tomllib is None:
    tomllib = FlimsyTomlParser

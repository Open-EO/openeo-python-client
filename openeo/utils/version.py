from __future__ import annotations

import contextlib
import re
from typing import Tuple, Union

# Type annotation aliases
_VersionTuple = Tuple[Union[int, str], ...]


class ComparableVersion:
    """
    Helper to compare a version (e.g. API version) against another (threshold) version

        >>> v = ComparableVersion('1.2.3')
        >>> v.at_least('1.2.1')
        True
        >>> v.at_least('1.10.2')
        False
        >>> v > "2.0"
        False

    To express a threshold condition you sometimes want the reference or threshold value on
    the left hand side or right hand side of the logical expression.
    There are two groups of methods to handle each case:

    - right hand side referencing methods. These read more intuitively. For example:

        `a.at_least(b)`: a is equal or higher than b
        `a.below(b)`: a is lower than b

    - left hand side referencing methods. These allow "currying" a threshold value
      in a reusable condition callable. For example:

        `a.or_higher(b)`: b is equal or higher than a
        `a.accept_lower(b)`: b is lower than a

    Implementation is loosely based on (now deprecated) `distutils.version.LooseVersion`,
    which pragmatically parses version strings as a sequence of numbers (compared numerically)
    or alphabetic strings (compared lexically), e.g.: 1.5.1, 1.5.2b2, 161, 8.02, 2g6, 2.2beta29.
    """

    _component_re = re.compile(r"(\d+ | [a-zA-Z]+ | \.)", re.VERBOSE)

    def __init__(self, version: Union[str, "ComparableVersion", tuple]):
        if isinstance(version, ComparableVersion):
            self._version = version._version
        elif isinstance(version, tuple):
            self._version = version
        elif isinstance(version, str):
            self._version = self._parse(version)
        else:
            raise ValueError(version)

    @classmethod
    def _parse(cls, version_string: str) -> _VersionTuple:
        components = [x for x in cls._component_re.split(version_string) if x and x != "."]
        for i, obj in enumerate(components):
            with contextlib.suppress(ValueError):
                components[i] = int(obj)
        return tuple(components)

    @property
    def parts(self) -> _VersionTuple:
        """Version components as a tuple"""
        return self._version

    def __repr__(self):
        return "{c}({v!r})".format(c=type(self).__name__, v=self._version)

    def __str__(self):
        return ".".join(map(str, self._version))

    def __hash__(self):
        return hash(self._version)

    def to_string(self):
        return str(self)

    @staticmethod
    def _pad(a: Union[str, ComparableVersion], b: Union[str, ComparableVersion]) -> Tuple[_VersionTuple, _VersionTuple]:
        """Pad version tuples with zero/empty to get same length for intuitive comparison"""
        a = ComparableVersion(a)._version
        b = ComparableVersion(b)._version
        if len(a) > len(b):
            b = b + tuple(0 if isinstance(x, int) else "" for x in a[len(b) :])
        elif len(b) > len(a):
            a = a + tuple(0 if isinstance(x, int) else "" for x in b[len(a) :])
        return a, b

    def __eq__(self, other: Union[str, ComparableVersion]) -> bool:
        a, b = self._pad(self, other)
        return a == b

    def __ge__(self, other: Union[str, ComparableVersion]) -> bool:
        a, b = self._pad(self, other)
        return a >= b

    def __gt__(self, other: Union[str, ComparableVersion]) -> bool:
        a, b = self._pad(self, other)
        return a > b

    def __le__(self, other: Union[str, ComparableVersion]) -> bool:
        a, b = self._pad(self, other)
        return a <= b

    def __lt__(self, other: Union[str, ComparableVersion]) -> bool:
        a, b = self._pad(self, other)
        return a < b

    def equals(self, other: Union[str, "ComparableVersion"]):
        return self == other

    # Right hand side referencing expressions.
    def at_least(self, other: Union[str, "ComparableVersion"]):
        """Self is at equal or higher than other."""
        return self >= other

    def above(self, other: Union[str, "ComparableVersion"]):
        """Self is higher than other."""
        return self > other

    def at_most(self, other: Union[str, "ComparableVersion"]):
        """Self is equal or lower than other."""
        return self <= other

    def below(self, other: Union[str, "ComparableVersion"]):
        """Self is lower than other."""
        return self < other

    # Left hand side referencing expressions.
    def or_higher(self, other: Union[str, "ComparableVersion"]):
        """Other is equal or higher than self."""
        return ComparableVersion(other) >= self

    def or_lower(self, other: Union[str, "ComparableVersion"]):
        """Other is equal or lower than self"""
        return ComparableVersion(other) <= self

    def accept_lower(self, other: Union[str, "ComparableVersion"]):
        """Other is lower than self."""
        return ComparableVersion(other) < self

    def accept_higher(self, other: Union[str, "ComparableVersion"]):
        """Other is higher than self."""
        return ComparableVersion(other) > self

    def require_at_least(self, other: Union[str, "ComparableVersion"]):
        """Raise exception if self is not at least other."""
        if not self.at_least(other):
            # TODO: eliminate openEO (API) reference here in generic ComparableVersion class
            raise ApiVersionException(f"openEO API version should be at least {other!s}, but got {self!s}.")


class ApiVersionException(RuntimeError):
    pass

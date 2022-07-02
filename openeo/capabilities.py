import contextlib
from abc import ABC
import re
from typing import Union

# Is this base class (still) useful?


class LooseVersion:
    """A version number consists of a series of numbers,
    separated by either periods or strings of letters. When comparing
    version numbers, the numeric components will be compared
    numerically, and the alphabetic components lexically. The following
    are all valid version numbers, in no particular order:
        1.5.1
        1.5.2b2
        161
        3.10a
        8.02
        3.4j
        1996.07.12
        3.2.pl0
        3.1.1.6
        2g6
        11g
        0.960923
        2.2beta29
        1.13++
        5.5.kw
        2.0b1pl0
    In fact, there is no such thing as an invalid version number under
    this scheme; the rules for comparison are simple and predictable.
    """

    component_re = re.compile(r'(\d+ | [a-z]+ | \.)', re.VERBOSE)

    def __init__(self, version_string=None):
        if isinstance(version_string, str):
            self.parse(version_string)

        if isinstance(version_string, LooseVersion):
            self.version = version_string.version

    def parse(self, version_string):
        self.vstring = version_string
        components = [x for x in self.component_re.split(version_string)
                      if x and x != '.']
        for i, obj in enumerate(components):
            with contextlib.suppress(ValueError):
                components[i] = int(obj)
        self.version = tuple(components)

    def __str__(self):
        return str(self.vstring)

    def __repr__(self):
        return "LooseVersion ('%s')" % str(self)

    def _cmp(self, other):
        if isinstance(other, str):
            other = LooseVersion(other)
        elif not isinstance(other, LooseVersion):
            return NotImplemented
        if self.version == other.version:
            return 0
        if self.version < other.version:
            return -1
        if self.version > other.version:
            return 1

    def __eq__(self, other):
        return self.version == LooseVersion(other).version

    def __ge__(self, other):
        return self.version >= LooseVersion(other).version

    def __gt__(self, other):
        return self.version > LooseVersion(other).version

    def __le__(self, other):
        return self.version <= LooseVersion(other).version

    def __lt__(self, other):
        return self.version < LooseVersion(other).version

    def equals(self, other):
        return self == other


class Capabilities(ABC):
    """Represents capabilities of a connection / back end."""

    def __init__(self, data):
        pass

    def version(self):
        """ Get openEO version. DEPRECATED: use api_version instead"""
        # Field: version
        # TODO: raise deprecation warning here?
        return self.api_version()

    def api_version(self) -> str:
        """Get OpenEO API version."""
        raise NotImplementedError

    @property
    def api_version_check(self) -> 'ComparableVersion':
        """Helper to easily check if the API version is at least or below some threshold version."""
        api_version = self.api_version()
        if not api_version:
            raise ApiVersionException("No API version found")
        return ComparableVersion(api_version)

    def list_features(self):
        """ List all supported features / endpoints."""
        # Field: endpoints
        pass

    def has_features(self, method_name):
        """ Check whether a feature / endpoint is supported."""
        # Field: endpoints > ...
        pass

    def currency(self):
        """ Get default billing currency."""
        # Field: billing > currency
        pass

    def list_plans(self):
        """ List all billing plans."""
        # Field: billing > plans
        pass


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
    """

    def __init__(self, version: Union[str, 'ComparableVersion']):
        if isinstance(version, ComparableVersion):
            self._version = version._version
        else:
            self._version = LooseVersion(version)

    def __repr__(self):
        return '{c}({v!r})'.format(c=type(self).__name__, v=self._version)

    def __str__(self):
        return str(self._version)

    def to_string(self):
        return str(self)

    def __eq__(self, other: Union[str, 'ComparableVersion']):
        return self._version == ComparableVersion(other)._version

    def __ge__(self, other: Union[str, 'ComparableVersion']):
        return self._version >= ComparableVersion(other)._version

    def __gt__(self, other: Union[str, 'ComparableVersion']):
        return self._version > ComparableVersion(other)._version

    def __le__(self, other: Union[str, 'ComparableVersion']):
        return self._version <= ComparableVersion(other)._version

    def __lt__(self, other: Union[str, 'ComparableVersion']):
        return self._version < ComparableVersion(other)._version

    def equals(self, other: Union[str, 'ComparableVersion']):
        return self == other

    # Right hand side referencing expressions.
    def at_least(self, other: Union[str, 'ComparableVersion']):
        """Self is at equal or higher than other."""
        return self >= other

    def above(self, other: Union[str, 'ComparableVersion']):
        """Self is higher than other."""
        return self > other

    def at_most(self, other: Union[str, 'ComparableVersion']):
        """Self is equal or lower than other."""
        return self <= other

    def below(self, other: Union[str, 'ComparableVersion']):
        """Self is lower than other."""
        return self < other

    # Left hand side referencing expressions.
    def or_higher(self, other: Union[str, 'ComparableVersion']):
        """Other is equal or higher than self."""
        return ComparableVersion(other) >= self

    def or_lower(self, other: Union[str, 'ComparableVersion']):
        """Other is equal or lower than self"""
        return ComparableVersion(other) <= self

    def accept_lower(self, other: Union[str, 'ComparableVersion']):
        """Other is lower than self."""
        return ComparableVersion(other) < self

    def accept_higher(self, other: Union[str, 'ComparableVersion']):
        """Other is higher than self."""
        return ComparableVersion(other) > self


class ApiVersionException(RuntimeError):
    pass

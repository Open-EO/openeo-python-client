from abc import ABC
from distutils.version import LooseVersion
from typing import Union


class Capabilities(ABC):
    """Represents capabilities of a connection / back end."""

    def __init__(self, data):
        pass

    def version(self):
        """ Get openEO version. DEPRECATED: use api_version instead"""
        # Field: version
        # TODO: raise deprecation warning here?
        return self.api_version()

    def api_version(self):
        """Get OpenEO API version."""
        # Field: api_version
        return

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
    """

    def __init__(self, version):
        self._version = LooseVersion(version)

    def __str__(self):
        return str(self._version)

    def to_string(self):
        return str(self)

    @classmethod
    def _to_loose_version(cls, version) -> LooseVersion:
        if isinstance(version, cls):
            return version._version
        elif isinstance(version, str):
            return LooseVersion(version)
        else:
            raise ValueError(version)

    def at_least(self, version: Union[str, 'ComparableVersion']):
        return self._version >= self._to_loose_version(version)

    def above(self, version: Union[str, 'ComparableVersion']):
        return self._version > self._to_loose_version(version)

    def at_most(self, version: Union[str, 'ComparableVersion']):
        return self._version <= self._to_loose_version(version)

    def below(self, version: Union[str, 'ComparableVersion']):
        return self._version < self._to_loose_version(version)


class ApiVersionException(RuntimeError):
    pass

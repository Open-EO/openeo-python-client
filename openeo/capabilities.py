from abc import ABC
from distutils.version import LooseVersion


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
    def api_version_check(self):
        """Helper to easily check if the API version is at least or below some threshold version."""
        return CheckableVersion(self.api_version())

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


class CheckableVersion:
    """
    Helper to check a version (e.g. API version) against another (threshold) version

    >>> v = CheckableVersion('1.2.3')
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

    def at_least(self, version):
        return self._version >= LooseVersion(version)

    def above(self, version):
        return self._version > LooseVersion(version)

    def at_most(self, version):
        return self._version <= LooseVersion(version)

    def below(self, version):
        return self._version < LooseVersion(version)

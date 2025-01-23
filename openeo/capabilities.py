from __future__ import annotations

from abc import ABC

from openeo.utils.version import ApiVersionException, ComparableVersion

__all__ = ["Capabilities", "ComparableVersion", "ApiVersionException"]


class Capabilities(ABC):
    """Represents capabilities of a connection / back end."""

    # TODO Is this base class (still) useful?

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
    def api_version_check(self) -> ComparableVersion:
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

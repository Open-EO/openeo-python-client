from typing import Dict, List, Optional, Union

from openeo.internal.jupyter import render_component
from openeo.rest.models import federation_extension
from openeo.util import deep_get
from openeo.utils.version import ApiVersionException, ComparableVersion

__all__ = ["OpenEoCapabilities"]


class OpenEoCapabilities:
    """Container of the openEO capabilities document of an openEO backend."""

    def __init__(self, data: dict, url: Optional[str] = None):
        self.capabilities = data
        self.url = url

    def get(self, key: str, default=None):
        return self.capabilities.get(key, default)

    def deep_get(self, *keys, default=None):
        return deep_get(self.capabilities, *keys, default=default)

    def api_version(self) -> Union[str, None]:
        """Version number of the openEO API specification this back-end implements."""
        if "api_version" in self.capabilities:
            return self.capabilities.get("api_version")
        else:
            # Legacy/deprecated
            return self.capabilities.get("version")

    @property
    def api_version_check(self) -> ComparableVersion:
        """Helper to easily check if the API version is at least or below some threshold version."""
        api_version = self.api_version()
        if not api_version:
            raise ApiVersionException("No API version found")
        return ComparableVersion(api_version)

    def supports_endpoint(self, path: str, method="GET") -> bool:
        """Check if backend supports given endpoint"""
        return any(
            endpoint.get("path") == path and method.upper() in endpoint.get("methods", [])
            for endpoint in self.capabilities.get("endpoints", [])
        )

    def currency(self) -> Union[str, None]:
        """Get default billing currency."""
        return self.deep_get("billing", "currency", default=None)

    def list_plans(self) -> List[dict]:
        """List all billing plans."""
        return self.deep_get("billing", "plans", default=[])

    def _repr_html_(self):
        return render_component("capabilities", data=self.capabilities, parameters={"url": self.url})

    def ext_federation_backend_details(self) -> Union[Dict[str, dict], None]:
        """
        Lists all back-ends (with details, such as URL) that are part of the federation
        if this backend acts as a federated backend,
        as specified in the openEO Federation Extension.
        Returns ``None`` otherwise.

        .. versionadded:: 0.38.0
        """
        return federation_extension.get_backend_details(data=self.capabilities)

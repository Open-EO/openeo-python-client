from typing import List, Optional

from openeo.capabilities import Capabilities
from openeo.internal.jupyter import render_component
from openeo.util import deep_get


class RESTCapabilities(Capabilities):
    """Represents REST capabilities of a connection / back end."""

    def __init__(self, data: dict, url: str = None):
        super(RESTCapabilities, self).__init__(data)
        self.capabilities = data
        self.url = url

    def get(self, key: str, default=None):
        return self.capabilities.get(key, default)

    def deep_get(self, *keys, default=None):
        return deep_get(self.capabilities, *keys, default=default)

    def api_version(self) -> str:
        """ Get openEO version."""
        if 'api_version' in self.capabilities:
            return self.capabilities.get('api_version')
        else:
            # Legacy/deprecated
            return self.capabilities.get('version')

    def list_features(self):
        """ List all supported features / endpoints."""
        return self.capabilities.get('endpoints')

    def has_features(self, method_name):
        """ Check whether a feature / endpoint is supported."""
        # Field: endpoints > ... TODO
        pass

    def supports_endpoint(self, path: str, method="GET"):
        return any(
            endpoint.get("path") == path and method.upper() in endpoint.get("methods", [])
            for endpoint in self.capabilities.get("endpoints", [])
        )

    def currency(self) -> Optional[str]:
        """Get default billing currency."""
        return self.deep_get("billing", "currency", default=None)

    def list_plans(self) -> List[dict]:
        """List all billing plans."""
        return self.deep_get("billing", "plans", default=[])

    def _repr_html_(self):
        return render_component("capabilities", data = self.capabilities, parameters = {"url": self.url})

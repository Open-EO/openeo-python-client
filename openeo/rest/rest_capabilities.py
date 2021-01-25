from openeo.capabilities import Capabilities
from openeo.internal.jupyter import render_component


class RESTCapabilities(Capabilities):
    """Represents REST capabilities of a connection / back end."""

    def __init__(self, data: dict, url: str = None):
        super(RESTCapabilities, self).__init__(data)
        self.capabilities = data
        self.url = url

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

    def currency(self):
        """ Get default billing currency."""
        return self.capabilities.get('billing', {}).get('currency')

    def list_plans(self):
        """ List all billing plans."""
        return self.capabilities.get('billing', {}).get('plans')

    def _repr_html_(self):
        return render_component("capabilities", data = self.capabilities, parameters = {"url": self.url})

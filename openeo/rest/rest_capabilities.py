from openeo.capabilities import Capabilities


class RESTCapabilities(Capabilities):
    """Represents REST capabilities of a connection / back end."""

    def __init__(self, data):
        self.capabilities = data

    def api_version(self):
        """ Get openEO version."""
        if 'api_version' in self.capabilities:
            return self.capabilities.get('api_version')
        else:
            # Legacy/deprecated
            return self.capabilities.get('version')

    def list_features(self):
        """ List all supported features / endpoints."""
        if "endpoints" in self.capabilities:
            return self.capabilities["endpoints"]

        return None

    def has_features(self, method_name):
        """ Check whether a feature / endpoint is supported."""
        # Field: endpoints > ... TODO
        pass

    def currency(self):
        """ Get default billing currency."""
        if "billing" in self.capabilities:
            if "currency" in self.capabilities["billing"]:
                return self.capabilities["billing"]["currency"]

        return None

    def list_plans(self):
        """ List all billing plans."""
        if "billing" in self.capabilities:
            if "plans" in self.capabilities["billing"]:
                return self.capabilities["billing"]["plans"]

        return None

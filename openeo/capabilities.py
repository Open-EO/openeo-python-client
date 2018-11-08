from abc import ABC


class Capabilities(ABC):
    """Represents capabilities of a connection / back end."""

    def __init__(self, data):
        pass

    def version(self):
        """ Get openEO version."""
        # Field: version
        pass

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

from abc import ABC


class Service(ABC):
    """Represents a process graph of openeo."""

    def __init__(self, service_id):
        pass

    def describe_service(self):
        """ Get all information about a secondary web service."""
        # GET /services/{service_id}
        pass

    def update_service(self, process_graph=None, title=None, description=None,
                       enabled=None, parameters=None, plan=None, budget=None):
        """ Update a secondary web service."""
        # PATCH /services/{service_id}
        pass

    def delete_service(self):
        """ Delete a secondary web service."""
        # DELETE /services/{service_id}
        pass

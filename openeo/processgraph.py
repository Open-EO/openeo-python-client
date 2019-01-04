from abc import ABC


class ProcessGraph(ABC):
    """Represents a process graph of openeo."""

    def __init__(self, pg_id):
        pass

    def describe_process_graph(self):
        """ Get all information about a stored process graph."""
        # GET /process_graphs/{pg_id}
        pass

    def update_process_graph(self, process_graph=None, title=None, description=None):
        """ Update a stored process graph."""
        # PATCH /process_graphs/{pg_id}
        pass

    def delete_process_graph(self):
        """ Delete a stored process graph."""
        # DELETE /process_graphs/{pg_id}
        pass

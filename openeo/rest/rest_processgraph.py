from openeo.processgraph import ProcessGraph
from openeo.connection import Connection


class RESTProcessgraph(ProcessGraph):
    """Represents REST process graph of a connection / back end."""

    def __init__(self, pg_id, connection: Connection):
        self.pg_id = pg_id
        self.connection = connection
        self.graph = None

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

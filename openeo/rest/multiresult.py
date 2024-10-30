from __future__ import annotations

from typing import Dict, List, Optional

from openeo import BatchJob
from openeo.internal.graph_building import FlatGraphableMixin, MultiLeafGraph
from openeo.rest import OpenEoClientException
from openeo.rest.connection import Connection, extract_connections


class MultiResult(FlatGraphableMixin):
    """
    Adapter to create/run batch jobs from process graphs with multiple end/result/leaf nodes.

    Usage example:

    .. code-block:: python

        cube1 = ...
        cube2 = ...
        multi_result = MultiResult([cube1, cube2])
        job = multi_result.create_job()


    """

    def __init__(self, leaves: List[FlatGraphableMixin], connection: Optional[Connection] = None):
        self._multi_leaf_graph = MultiLeafGraph(leaves=leaves)
        self._connection = self._common_connection(leaves=leaves, connection=connection)

    @staticmethod
    def _common_connection(leaves: List[FlatGraphableMixin], connection: Optional[Connection] = None) -> Connection:
        """Find common connection. Fails if there are multiple or none."""
        connections = set()
        if connection:
            connections.add(connection)
        connections.update(extract_connections(leaves))

        if len(connections) == 1:
            return connections.pop()
        elif len(connections) == 0:
            raise OpenEoClientException("No connection in any of the MultiResult leaves")
        else:
            raise OpenEoClientException("MultiResult with multiple different connections")

    def flat_graph(self) -> Dict[str, dict]:
        return self._multi_leaf_graph.flat_graph()

    def create_job(
        self,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
    ) -> BatchJob:
        return self._connection.create_job(
            process_graph=self._multi_leaf_graph,
            title=title,
            description=description,
            additional=job_options,
            validate=validate,
        )

    def execute_batch(
        self,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
    ) -> BatchJob:
        job = self.create_job(title=title, description=description, job_options=job_options, validate=validate)
        return job.run_synchronous()

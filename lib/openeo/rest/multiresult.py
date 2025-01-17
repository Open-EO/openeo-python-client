from __future__ import annotations

from typing import Dict, List, Optional

from openeo import BatchJob
from openeo.internal.graph_building import FlatGraphableMixin, MultiLeafGraph
from openeo.rest import OpenEoClientException
from openeo.rest.connection import Connection, extract_connections


class MultiResult(FlatGraphableMixin):
    """
    Helper to create and run batch jobs with process graphs
    that contain multiple result nodes
    or, more generally speaking, multiple process graph "leaf" nodes.

    Provide multiple
    :py:class:`~openeo.rest.datacube.DataCube`/:py:class:`~openeo.rest.vectorcube.VectorCube`
    instances to the constructor,
    and start a batch job from that,
    for example as follows:

    .. code-block:: python

        from openeo import MultiResult

        cube1 = ...
        cube2 = ...
        multi_result = MultiResult([cube1, cube2])
        job = multi_result.create_job()

    .. seealso::

        :ref:`multi-result-process-graphs`

    .. versionadded:: 0.35.0
    """

    __slots__ = ("_multi_leaf_graph", "_connection")

    def __init__(self, leaves: List[FlatGraphableMixin], connection: Optional[Connection] = None):
        """
        Build a :py:class:`MultiResult` instance from multiple leaf nodes

        :param leaves: list of objects that can be
            converted to an openEO-style (flat) process graph representation,
            typically :py:class:`~openeo.rest.datacube.DataCube`
            or :py:class:`~openeo.rest.vectorcube.VectorCube` instances.
        :param connection: Optional connection to use for creating/starting batch jobs,
            for special use cases where the provided leaf instances
            are not already associated with a connection.
        """
        self._multi_leaf_graph = MultiLeafGraph(leaves=leaves)
        self._connection = self._extract_connection(leaves=leaves, connection=connection)

    @staticmethod
    def _extract_connection(leaves: List[FlatGraphableMixin], connection: Optional[Connection] = None) -> Connection:
        """
        Extract common connection from leaves and/or explicitly provided connection.
        Fails if there are multiple or none.
        """
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
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
        log_level: Optional[str] = None,
    ) -> BatchJob:
        return self._connection.create_job(
            process_graph=self._multi_leaf_graph,
            title=title,
            description=description,
            additional=additional,
            job_options=job_options,
            validate=validate,
            log_level=log_level,
        )

    def execute_batch(
        self,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
        log_level: Optional[str] = None,
    ) -> BatchJob:
        job = self.create_job(
            title=title,
            description=description,
            additional=additional,
            job_options=job_options,
            validate=validate,
            log_level=log_level,
        )
        return job.run_synchronous()

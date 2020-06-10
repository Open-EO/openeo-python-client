from typing import Dict

from openeo.internal.graph_building import PGNode
from openeo.metadata import CollectionMetadata


class VectorCube():
    """
    A Vector Cube, or 'Vector Collection' is a data structure containing 'Features':
    https://www.w3.org/TR/sdw-bp/#dfn-feature

    The features in this cube are restricted to have a geometry. Geometries can be points, lines, polygons etcetera.
    A geometry is specified in a 'coordinate reference system'. https://www.w3.org/TR/sdw-bp/#dfn-coordinate-reference-system-(crs)
    """

    def __init__(self, graph: PGNode, connection: 'Connection', metadata: CollectionMetadata = None):
        super().__init__(metadata=metadata)
        # Process graph
        self._pg = graph
        self._connection = connection
        self.metadata = metadata

    def __str__(self):
        return "DataCube({pg})".format(pg=self._pg)

    @property
    def graph(self) -> dict:
        """Get the process graph in flattened dict representation"""
        return self.flatten()

    def flatten(self) -> dict:
        """Get the process graph in flattened dict representation"""
        return self._pg.flatten()

    @property
    def _api_version(self):
        return self._connection.capabilities().api_version_check

    @property
    def connection(self):
        return self._connection

    def process(self, process_id: str, args: dict = None, metadata: CollectionMetadata = None, **kwargs) -> 'DataCube':
        """
        Generic helper to create a new DataCube by applying a process.

        :param process_id: process id of the process.
        :param args: argument dictionary for the process.
        :return: new DataCube instance
        """
        return self.process_with_node(PGNode(
            process_id=process_id,
            arguments=args, **kwargs
        ), metadata=metadata)


    def process_with_node(self, pg: PGNode, metadata: CollectionMetadata = None) -> 'DataCube':
        """
        Generic helper to create a new DataCube by applying a process (given as process graph node)

        :param pg: process graph node (containing process id and arguments)
        :param metadata: (optional) metadata to override original cube metadata (e.g. when reducing dimensions)
        :return: new DataCube instance
        """
        # TODO: deep copy `self.metadata` instead of using same instance?
        # TODO: cover more cases where metadata has to be altered
        return VectorCube(graph=pg, connection=self._connection, metadata=metadata or self.metadata)

    def save_result(self, format: str = "GeoJson", options: dict = None):
        return self.process(
            process_id="save_result",
            args={
                "data": {"from_node": self._pg},
                "format": format,
                "options": options or {}
            }
        )


    def download(self, outputfile: str, format: str = "GeoJson", options: dict = None):
        newcollection = self.save_result(format=format, options=options)
        return self._connection.download(newcollection._pg.flatten(), outputfile)

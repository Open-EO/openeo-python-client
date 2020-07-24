from typing import Dict

from openeo.internal.graph_building import PGNode
from openeo.metadata import CollectionMetadata
from typing import Union
import pathlib
from openeo.rest.job import RESTJob


class VectorCube():
    """
    A Vector Cube, or 'Vector Collection' is a data structure containing 'Features':
    https://www.w3.org/TR/sdw-bp/#dfn-feature

    The features in this cube are restricted to have a geometry. Geometries can be points, lines, polygons etcetera.
    A geometry is specified in a 'coordinate reference system'. https://www.w3.org/TR/sdw-bp/#dfn-coordinate-reference-system-(crs)
    """

    def __init__(self, graph: PGNode, connection: 'Connection', metadata: CollectionMetadata = None):
        super().__init__()
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

    def process(self, process_id: str, args: dict = None, metadata: CollectionMetadata = None, **kwargs) -> 'VectorCube':
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

    def process_with_node(self, pg: PGNode, metadata: CollectionMetadata = None) -> 'VectorCube':
        """
        Generic helper to create a new DataCube by applying a process (given as process graph node)

        :param pg: process graph node (containing process id and arguments)
        :param metadata: (optional) metadata to override original cube metadata (e.g. when reducing dimensions)
        :return: new DataCube instance
        """
        from openeo.rest.datacube import DataCube, THIS
        arguments = pg.arguments
        for k, v in arguments.items():
            if isinstance(v, DataCube) or isinstance(v, VectorCube):
                arguments[k] = {"from_node": v._pg}
            elif v is THIS:
                arguments[k] = {"from_node": self._pg}
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


    def execute_batch(
            self,
            outputfile: Union[str, pathlib.Path], out_format: str = None,
            print=print, max_poll_interval=60, connection_retry_interval=30,
            job_options=None, **format_options) -> RESTJob:
        """
        Evaluate the process graph by creating a batch job, and retrieving the results when it is finished.
        This method is mostly recommended if the batch job is expected to run in a reasonable amount of time.

        For very long running jobs, you probably do not want to keep the client running.

        :param job_options:
        :param outputfile: The path of a file to which a result can be written
        :param out_format: (optional) Format of the job result.
        :param format_options: String Parameters for the job result format

        """
        job = self.send_job(out_format, job_options=job_options, **format_options)
        return job.run_synchronous(
            # TODO #135 support multi file result sets too
            outputfile=outputfile,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )

    def send_job(self, out_format=None, job_options=None, **format_options) -> RESTJob:
        """
        Sends a job to the backend and returns a ClientJob instance.

        :param out_format: String Format of the job result.
        :param job_options:
        :param format_options: String Parameters for the job result format
        :return: status: ClientJob resulting job.
        """
        shp = self
        if out_format:
            # add `save_result` node
            shp = shp.save_result(format=out_format, options=format_options)
        return self._connection.create_job(process_graph=shp.graph, additional=job_options)

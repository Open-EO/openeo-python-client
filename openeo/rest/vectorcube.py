import json
import pathlib
from typing import Union, Optional
import typing

from openeo.internal.graph_building import PGNode, _FromNodeMixin
from openeo.metadata import CollectionMetadata
from openeo.rest._datacube import _ProcessGraphAbstraction, THIS
from openeo.rest.job import RESTJob
from openeo.util import legacy_alias

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime). `hasattr` is Python 3.5 workaround #210
    from openeo import Connection


class VectorCube(_ProcessGraphAbstraction):
    """
    A Vector Cube, or 'Vector Collection' is a data structure containing 'Features':
    https://www.w3.org/TR/sdw-bp/#dfn-feature

    The features in this cube are restricted to have a geometry. Geometries can be points, lines, polygons etcetera.
    A geometry is specified in a 'coordinate reference system'. https://www.w3.org/TR/sdw-bp/#dfn-coordinate-reference-system-(crs)
    """

    def __init__(self, graph: PGNode, connection: 'Connection', metadata: CollectionMetadata = None):
        super().__init__(pgnode=graph, connection=connection)
        # TODO: does VectorCube need CollectionMetadata?
        self.metadata = metadata

    def process(
            self,
            process_id: str,
            arguments: dict = None,
            metadata: Optional[CollectionMetadata] = None,
            namespace: Optional[str] = None,
            **kwargs) -> 'VectorCube':
        """
        Generic helper to create a new DataCube by applying a process.

        :param process_id: process id of the process.
        :param args: argument dictionary for the process.
        :return: new DataCube instance
        """
        pg = self._build_pgnode(process_id=process_id, arguments=arguments, namespace=namespace, **kwargs)
        return VectorCube(graph=pg, connection=self._connection, metadata=metadata or self.metadata)

    def process_with_node(self, pg: PGNode, metadata: CollectionMetadata = None) -> 'VectorCube':
        """
        Generic helper to create a new DataCube by applying a process (given as process graph node)

        :param pg: process graph node (containing process id and arguments)
        :param metadata: (optional) metadata to override original cube metadata (e.g. when reducing dimensions)
        :return: new DataCube instance
        """
        arguments = pg.arguments
        for k, v in arguments.items():
            # TODO: it's against intended flow to resolve THIS and _FromNodeMixin at this point (should be done before building PGNode)
            if v is THIS:
                v = self
            if isinstance(v, _FromNodeMixin):
                arguments[k] = {"from_node": v.from_node()}
        # TODO: deep copy `self.metadata` instead of using same instance?
        return VectorCube(graph=pg, connection=self._connection, metadata=metadata or self.metadata)

    def save_result(self, format: str = "GeoJson", options: dict = None):
        return self.process(
            process_id="save_result",
            arguments={
                "data": self,
                "format": format,
                "options": options or {}
            }
        )

    def download(self, outputfile: str, format: str = "GeoJson", options: dict = None):
        cube = self.save_result(format=format, options=options)
        return self._connection.download(cube.flat_graph(), outputfile)

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
        job = self.create_job(out_format, job_options=job_options, **format_options)
        return job.run_synchronous(
            # TODO #135 support multi file result sets too
            outputfile=outputfile,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )

    def create_job(self, out_format=None, job_options=None, **format_options) -> RESTJob:
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
        return self._connection.create_job(process_graph=shp.flat_graph(), additional=job_options)

    send_job = legacy_alias(create_job, name="send_job")

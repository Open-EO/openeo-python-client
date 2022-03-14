import json
import pathlib
from typing import Union, Optional
import typing

from openeo.internal.graph_building import PGNode
from openeo.rest.job import RESTJob
from openeo.util import legacy_alias


if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime). `hasattr` is Python 3.5 workaround #210
    from openeo import Connection


class MlModel:
    """
    A machine learning model accompanied with STAC metadata, including the ml-model extension.
    """
    def __init__(self, graph: PGNode, connection: 'Connection'):
        super().__init__()
        self._pg = graph
        self._connection = connection

    def __str__(self):
        return "MlModel({pg})".format(pg=self._pg)

    @property
    def graph(self) -> dict:
        """Get the process graph in flat dict representation"""
        return self.flat_graph()

    def flat_graph(self) -> dict:
        """Get the process graph in flat dict representation"""
        return self._pg.flat_graph()

    flatten = legacy_alias(flat_graph, name="flatten")

    def to_json(self, indent=2, separators=None) -> str:
        """
        Get JSON representation of (flat dict) process graph.
        """
        pg = {"process_graph": self.flat_graph()}
        return json.dumps(pg, indent=indent, separators=separators)

    @property
    def _api_version(self):
        return self._connection.capabilities().api_version_check

    @property
    def connection(self):
        return self._connection

    def save_ml_model(self, options: Optional[dict] = None):
        pgnode = PGNode(
            process_id="save_ml_model",
            arguments={
                "data": {'from_node': self._pg},
                "options": options or {}
            }
        )
        return MlModel(graph=pgnode, connection=self._connection)

    def execute_batch(
            self,
            outputfile: Union[str, pathlib.Path],
            print=print, max_poll_interval=60, connection_retry_interval=30,
            job_options=None,
    ) -> RESTJob:
        """
        Evaluate the process graph by creating a batch job, and retrieving the results when it is finished.
        This method is mostly recommended if the batch job is expected to run in a reasonable amount of time.

        For very long running jobs, you probably do not want to keep the client running.

        :param job_options:
        :param outputfile: The path of a file to which a result can be written
        :param out_format: (optional) Format of the job result.
        :param format_options: String Parameters for the job result format

        """
        # TODO: check/warn about final `save_ml_model` node?
        job = self.send_job(additional=job_options)
        return job.run_synchronous(
            # TODO #135 support multi file result sets too
            outputfile=outputfile,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )

    def send_job(self, **kwargs) -> RESTJob:
        """
        Sends a job to the backend and returns a ClientJob instance.

        See :py:meth:`Connection.create_job` for additional arguments (e.g. to set job title, description, ...)

        :return: resulting job.
        """
        return self._connection.create_job(process_graph=self.flat_graph(), **kwargs)


import pathlib
import typing
from typing import Union, Optional

from openeo.internal.graph_building import PGNode
from openeo.rest._datacube import _ProcessGraphAbstraction
from openeo.rest.job import RESTJob

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime). `hasattr` is Python 3.5 workaround #210
    from openeo import Connection


class MlModel(_ProcessGraphAbstraction):
    """
    A machine learning model.

    It is the result of a training procedure, e.g. output of a ``fit_...`` process,
    and can be used for prediction (classification or regression) with the corresponding ``predict_...`` process.
    """
    def __init__(self, graph: PGNode, connection: 'Connection'):
        super().__init__(pgnode=graph, connection=connection)

    def save_ml_model(self, options: Optional[dict] = None):
        pgnode = PGNode(
            process_id="save_ml_model",
            arguments={"data": self, "options": options or {}}
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
        job = self.create_job(additional=job_options)
        return job.run_synchronous(
            # TODO #135 support multi file result sets too
            outputfile=outputfile,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )

    def create_job(self, **kwargs) -> RESTJob:
        """
        Sends a job to the backend and returns a ClientJob instance.

        See :py:meth:`Connection.create_job` for additional arguments (e.g. to set job title, description, ...)

        :return: resulting job.
        """
        return self._connection.create_job(process_graph=self.flat_graph(), **kwargs)

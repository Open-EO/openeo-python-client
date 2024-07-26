from __future__ import annotations

import logging
import pathlib
import typing
from typing import Optional, Union

from openeo.internal.documentation import openeo_process
from openeo.internal.graph_building import PGNode
from openeo.rest._datacube import _ProcessGraphAbstraction
from openeo.rest.job import BatchJob

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo import Connection

_log = logging.getLogger(__name__)


class MlModel(_ProcessGraphAbstraction):
    """
    A machine learning model.

    It is the result of a training procedure, e.g. output of a ``fit_...`` process,
    and can be used for prediction (classification or regression) with the corresponding ``predict_...`` process.

    .. versionadded:: 0.10.0
    """

    def __init__(self, graph: PGNode, connection: Connection):
        super().__init__(pgnode=graph, connection=connection)

    def save_ml_model(self, options: Optional[dict] = None):
        """
        Saves a machine learning model as part of a batch job.

        :param options: Additional parameters to create the file(s).
        """
        pgnode = PGNode(
            process_id="save_ml_model",
            arguments={"data": self, "options": options or {}}
        )
        return MlModel(graph=pgnode, connection=self._connection)

    @staticmethod
    @openeo_process
    def load_ml_model(connection: Connection, id: Union[str, BatchJob]) -> MlModel:
        """
        Loads a machine learning model from a STAC Item.

        :param connection: connection object
        :param id: STAC item reference, as URL, batch job (id) or user-uploaded file
        :return:

        .. versionadded:: 0.10.0
        """
        if isinstance(id, BatchJob):
            id = id.job_id
        return MlModel(graph=PGNode(process_id="load_ml_model", id=id), connection=connection)

    def execute_batch(
            self,
            outputfile: Union[str, pathlib.Path],
            print=print, max_poll_interval=60, connection_retry_interval=30,
            job_options=None,
    ) -> BatchJob:
        """
        Evaluate the process graph by creating a batch job, and retrieving the results when it is finished.
        This method is mostly recommended if the batch job is expected to run in a reasonable amount of time.

        For very long running jobs, you probably do not want to keep the client running.

        :param job_options:
        :param outputfile: The path of a file to which a result can be written
        :param out_format: (optional) Format of the job result.
        :param format_options: String Parameters for the job result format
        """
        job = self.create_job(job_options=job_options)
        return job.run_synchronous(
            # TODO #135 support multi file result sets too
            outputfile=outputfile,
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )

    def create_job(
        self,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        plan: Optional[str] = None,
        budget: Optional[float] = None,
        job_options: Optional[dict] = None,
    ) -> BatchJob:
        """
        Sends a job to the backend and returns a ClientJob instance.

        :param title: job title
        :param description: job description
        :param plan: The billing plan to process and charge the job with
        :param budget: Maximum budget to be spent on executing the job.
            Note that some backends do not honor this limit.
        :param job_options: A dictionary containing (custom) job options
        :param format_options: String Parameters for the job result format
        :return: Created job.
        """
        # TODO: centralize `create_job` for `DataCube`, `VectorCube`, `MlModel`, ...
        pg = self
        if pg.result_node().process_id not in {"save_ml_model"}:
            _log.warning("Process graph has no final `save_ml_model`. Adding it automatically.")
            pg = pg.save_ml_model()
        return self._connection.create_job(
            process_graph=pg.flat_graph(),
            title=title,
            description=description,
            plan=plan,
            budget=budget,
            additional=job_options,
        )

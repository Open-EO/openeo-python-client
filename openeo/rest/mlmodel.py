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

    def __init__(self, graph: PGNode, connection: Union[Connection, None]):
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
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        plan: Optional[str] = None,
        budget: Optional[float] = None,
        print=print,
        max_poll_interval=60,
        connection_retry_interval=30,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        show_error_logs: bool = True,
        log_level: Optional[str] = None,
    ) -> BatchJob:
        """
        Evaluate the process graph by creating a batch job, and retrieving the results when it is finished.
        This method is mostly recommended if the batch job is expected to run in a reasonable amount of time.

        For very long-running jobs, you probably do not want to keep the client running.

        :param job_options:
        :param outputfile: The path of a file to which a result can be written
        :param out_format: (optional) Format of the job result.
        :param format_options: String Parameters for the job result format
        :param additional: additional (top-level) properties to set in the request body
        :param job_options: dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param show_error_logs: whether to automatically print error logs when the batch job failed.
        :param log_level: Optional minimum severity level for log entries that the back-end should keep track of.
            One of "error" (highest severity), "warning", "info", and "debug" (lowest severity).

        .. versionchanged:: 0.36.0
            Added argument ``additional``.

        .. versionchanged:: 0.37.0
            Added argument ``show_error_logs``.

        .. versionchanged:: 0.37.0
            Added argument ``log_level``.
        """
        job = self.create_job(
            title=title,
            description=description,
            plan=plan,
            budget=budget,
            additional=additional,
            job_options=job_options,
            log_level=log_level,
        )
        return job.run_synchronous(
            # TODO #135 support multi file result sets too
            outputfile=outputfile,
            print=print,
            max_poll_interval=max_poll_interval,
            connection_retry_interval=connection_retry_interval,
            show_error_logs=show_error_logs,
        )

    def create_job(
        self,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        plan: Optional[str] = None,
        budget: Optional[float] = None,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        log_level: Optional[str] = None,
    ) -> BatchJob:
        """
        Sends a job to the backend and returns a ClientJob instance.

        :param title: job title
        :param description: job description
        :param plan: The billing plan to process and charge the job with
        :param budget: Maximum budget to be spent on executing the job.
            Note that some backends do not honor this limit.
        :param additional: additional (top-level) properties to set in the request body
        :param job_options: dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param format_options: String Parameters for the job result format
        :param log_level: Optional minimum severity level for log entries that the back-end should keep track of.
            One of "error" (highest severity), "warning", "info", and "debug" (lowest severity).
        :return: Created job.

        .. versionchanged:: 0.36.0
            Added argument ``additional``.

        .. versionchanged:: 0.37.0
            Added argument ``log_level``.
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
            additional=additional,
            job_options=job_options,
            log_level=log_level,
        )

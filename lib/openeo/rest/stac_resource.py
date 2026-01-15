from __future__ import annotations

import typing
from pathlib import Path
from typing import Callable, Mapping, Optional, Union

from openeo.internal.documentation import openeo_process
from openeo.internal.graph_building import PGNode
from openeo.rest import (
    DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
    DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
)
from openeo.rest._datacube import _ProcessGraphAbstraction
from openeo.rest.job import BatchJob

if typing.TYPE_CHECKING:
    from openeo.rest.connection import Connection


class StacResource(_ProcessGraphAbstraction):
    """
    Handle for a progress graph node that represents a STAC resource (object with subtype "stac"),
    e.g. as returned by openEO process ``save_result``,
    or handled by openEO processes ``export_workspace``/``stac_modify``.

    Refers to a STAC resource of any type (Catalog, Collection, or Item).
    It can refer to:

    - static STAC resources, e.g. hosted on cloud storage
    - dynamic STAC resources made available via a STAC API
    - a STAC JSON representation embedded as an argument into an openEO user-defined process

    .. versionadded:: 0.39.0
    """

    def __init__(self, graph: PGNode, connection: Optional[Connection] = None):
        super().__init__(pgnode=graph, connection=connection)

    def process(
        self,
        process_id: str,
        arguments: Optional[dict] = None,
        namespace: Optional[str] = None,
        **kwargs,
    ) -> StacResource:
        """
        Generic helper to create a new StacResource by applying a process.

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :param namespace: optional: process namespace

        .. versionadded:: 0.39.1
        """
        pg = self._build_pgnode(process_id=process_id, arguments=arguments, namespace=namespace, **kwargs)
        # TODO: warn that actual return type can not or is not properly detected
        return StacResource(graph=pg, connection=self._connection)

    @openeo_process
    def export_workspace(self, workspace: str, merge: Union[str, None] = None) -> StacResource:
        """
        Export data to a cloud user workspace

        Exports the given processing results made available through a STAC resource
        (e.g., a STAC Collection) to the given user workspace.
        The STAC resource itself is exported with all STAC resources and assets underneath.

        :param workspace: The identifier of the workspace to export to.
        :param merge: (optional) Provides a cloud-specific path identifier
            to a STAC resource to merge the given STAC resource into.
            If not provided, the STAC resource is kept separate
            from any other STAC resources in the workspace.

        :return: the potentially updated STAC resource.
        """
        return StacResource(
            graph=self._build_pgnode(
                process_id="export_workspace", arguments={"data": self, "workspace": workspace, "merge": merge}
            ),
            connection=self._connection,
        )

    def download(
        self,
        outputfile: Optional[Union[str, Path]] = None,
        *,
        validate: Optional[bool] = None,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        on_response_headers: Optional[Callable[[Mapping], None]] = None,
    ):
        """
        Send the underlying process graph to the backend
        for synchronous processing and directly download the result.

        If ``outputfile`` is provided, the result is downloaded to that path.
        Otherwise a :py:class:`bytes` object is returned with the raw data.

        :param outputfile: (optional) output path to download to.
        :param validate: (optional) toggle to enable/prevent validation of the process graphs before execution
            (overruling the connection's ``auto_validate`` setting).
        :param additional: (optional) additional (top-level) properties to set in the request body
        :param job_options: (optional) dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param on_response_headers: (optional) callback to handle/show the response headers

        :return: if ``outputfile`` was not specified:
            a :py:class:`bytes` object containing the raw data.
            Otherwise, ``None`` is returned.

        .. versionchanged:: 0.40
            Added argument ``on_response_headers``.
        """
        return self._connection.download(
            graph=self.flat_graph(),
            outputfile=outputfile,
            validate=validate,
            additional=additional,
            job_options=job_options,
            on_response_headers=on_response_headers,
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
        validate: Optional[bool] = None,
        log_level: Optional[str] = None,
    ) -> BatchJob:
        """
        Send the underlying process graph to the backend
        to create an openEO batch job
        and return a corresponding :py:class:`~openeo.rest.job.BatchJob` instance.

        Note that this method only *creates* the openEO batch job at the backend,
        but it does not *start* it.
        Use :py:meth:`execute_batch` instead to let the openEO Python client
        take care of the full job life cycle: create, start and track its progress until completion.

        :param title: (optional) job title.
        :param description: (optional) job description.
        :param plan: (optional) the billing plan to process and charge the job with.
        :param budget: (optional) maximum budget to be spent on executing the job.
            Note that some backends do not honor this limit.
        :param additional: (optional) additional (top-level) properties to set in the request body
        :param job_options: (optional) dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param validate: (optional) toggle to enable/prevent validation of the process graphs before execution
            (overruling the connection's ``auto_validate`` setting).
        :param log_level: (optional) minimum severity level for log entries that the back-end should keep track of.
            One of "error" (highest severity), "warning", "info", and "debug" (lowest severity).

        :return: Handle to the job created at the backend.
        """
        return self._connection.create_job(
            process_graph=self.flat_graph(),
            title=title,
            description=description,
            plan=plan,
            budget=budget,
            validate=validate,
            additional=additional,
            job_options=job_options,
            log_level=log_level,
        )

    def execute_batch(
        self,
        outputfile: Optional[Union[str, Path]] = None,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        plan: Optional[str] = None,
        budget: Optional[float] = None,
        print: typing.Callable[[str], None] = print,
        max_poll_interval: float = DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
        connection_retry_interval: float = DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
        additional: Optional[dict] = None,
        job_options: Optional[dict] = None,
        validate: Optional[bool] = None,
        show_error_logs: bool = True,
        log_level: Optional[str] = None,
    ) -> BatchJob:
        """
        Execute the underlying process graph at the backend in batch job mode:

        - create the job (like :py:meth:`create_job`)
        - start the job (like :py:meth:`BatchJob.start() <openeo.rest.job.BatchJob.start>`)
        - track the job's progress with an active polling loop
          (like :py:meth:`BatchJob.run_synchronous() <openeo.rest.job.BatchJob.run_synchronous>`)
        - optionally (if ``outputfile`` is specified) download the job's results
          when the job finished successfully

        .. note::
            Because of the active polling loop,
            which blocks any further progress of your script or application,
            this :py:meth:`execute_batch` method is mainly recommended
            for batch jobs that are expected to complete
            in a time that is reasonable for your use case.

        :param outputfile: (optional) output path to download to.
        :param title: (optional) job title.
        :param description: (optional) job description.
        :param plan: (optional) the billing plan to process and charge the job with.
        :param budget: (optional) maximum budget to be spent on executing the job.
            Note that some backends do not honor this limit.
        :param additional: (optional) additional (top-level) properties to set in the request body
        :param job_options: (optional) dictionary of job options to pass to the backend
            (under top-level property "job_options")
        :param validate: (optional) toggle to enable/prevent validation of the process graphs before execution
            (overruling the connection's ``auto_validate`` setting).
        :param log_level: (optional) minimum severity level for log entries that the back-end should keep track of.
            One of "error" (highest severity), "warning", "info", and "debug" (lowest severity).
        :param print: print/logging function to show progress/status
        :param max_poll_interval: maximum number of seconds to sleep between job status polls
        :param connection_retry_interval: how long to wait when status poll failed due to connection issue
        :param show_error_logs: whether to automatically print error logs when the batch job failed.

        :return: Handle to the job created at the backend.
        """
        job = self.create_job(
            title=title,
            description=description,
            plan=plan,
            budget=budget,
            additional=additional,
            job_options=job_options,
            validate=validate,
            log_level=log_level,
        )
        job.start_and_wait(
            print=print,
            max_poll_interval=max_poll_interval,
            connection_retry_interval=connection_retry_interval,
            show_error_logs=show_error_logs,
        )
        if outputfile is not None:
            job.download_result(target=outputfile)
        return job

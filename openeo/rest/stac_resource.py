from __future__ import annotations

import typing
from pathlib import Path
from typing import Optional, Union

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
    e.g. as returned by `save_result`,  or handled by `export_workspace`/`stac_modify`.


    Refers to a STAC resource of any type (Catalog, Collection, or Item).
    It can refer to:
    - static STAC resources, e.g. hosted on cloud storage
    - dynamic STAC resources made available via a STAC API
    - a STAC JSON representation embedded as an argument into an openEO user-defined process
    """

    def __init__(self, graph: PGNode, connection: Optional[Connection] = None):
        super().__init__(pgnode=graph, connection=connection)

    @openeo_process
    def export_workspace(self, workspace: str, merge: Union[str, None] = None) -> StacResource:
        """
        Export data to a cloud user workspace

        Exports the given processing results made available through a STAC resource
        (e.g., a STAC Collection) to the given user workspace.
        The STAC resource itself is exported with all STAC resources and assets underneath.

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
    ):
        """TODO"""
        return self._connection.download(
            graph=self.flat_graph(),
            outputfile=outputfile,
            validate=validate,
            additional=additional,
            job_options=job_options,
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
        """TODO"""
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
        auto_add_save_result: bool = True,
        show_error_logs: bool = True,
        log_level: Optional[str] = None,
    ) -> BatchJob:
        """TODO"""
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
        return job.run_synchronous(
            outputfile=outputfile,
            print=print,
            max_poll_interval=max_poll_interval,
            connection_retry_interval=connection_retry_interval,
            show_error_logs=show_error_logs,
        )

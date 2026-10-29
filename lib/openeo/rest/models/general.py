from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Union

from openeo.internal.jupyter import render_component
from openeo.rest.models import federation_extension
from openeo.rest.models.logs import LogEntry, normalize_log_level

if TYPE_CHECKING:
    from openeo.rest.connection import Connection

@dataclass(frozen=True)
class Link:
    """
    Container for (web) link data, used throughout the openEO API,
    to point to alternate representations, a license, extra detailed information, and more.
    """

    rel: str
    href: str
    type: Optional[str] = None
    title: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> Link:
        """Build :py:class:`Link` from dictionary (e.g. parsed JSON representation)."""
        return cls(rel=data["rel"], href=data["href"], type=data.get("type"), title=data.get("title"))

    # TODO: add _html_repr_ for Jupyter integration
    # TODO: also provide container for list of links with methods to easily look up by `rel` or `type`


class CollectionListingResponse(list):
    """
    Container for collection metadata listing received
    from a ``GET /collections`` request.

    .. note::
        This object mimics, for backward compatibility reasons,
        the interface of simple list of collection metadata dictionaries (``List[dict]``),
        which was the original return API of
        :py:meth:`~openeo.rest.connection.Connection.list_collections()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``GET /collections`` request
    :param connection: optional connection object to use for federation extension

    .. seealso:: :py:meth:`openeo.rest.connection.Connection.list_collections()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data", "_connection"]

    def __init__(self, response_data: dict, connection: Optional[Connection] = None):
        self._data = response_data
        # Mimic original list of collection metadata dictionaries
        super().__init__(response_data["collections"])

        self._connection = connection
        self.ext_federation_missing(auto_warn=True)

    def _repr_html_(self):
        federation = self._connection.capabilities().ext_federation_backend_details() if self._connection else None
        return render_component(
            component="collections",
            data=self,
            parameters={
                "missing": self.ext_federation_missing(),
                "federation": federation,
            },
        )

    @property
    def links(self) -> List[Link]:
        """Get links related to this resource."""
        return [Link.from_dict(d) for d in self._data.get("links", [])]

    def ext_federation_missing(self, *, auto_warn: bool = False) -> Union[None, List[str]]:
        """
        List of backends IDs (from the federation)
        that were not available during the resource listing request.

        :param auto_warn: whether to automatically log a warning if missing federation components are detected.

        .. seealso:: :ref:`federation-extension`

        .. warning:: this API is experimental and subject to change.
        """
        return federation_extension.get_federation_missing(
            data=self._data, resource_name="collection listing", auto_warn=auto_warn
        )


class ProcessListingResponse(list):
    """
    Container for process metadata listing received
    from a ``GET /processes`` request.

    .. note::
        This object mimics, for backward compatibility reasons,
        the interface of simple list of process metadata dictionaries (``List[dict]``),
        :py:meth:`~openeo.rest.connection.Connection.list_processes()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``GET /processes`` request
    :param connection: optional connection object to use for federation extension

    .. seealso:: :py:meth:`openeo.rest.connection.Connection.list_processes()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data", "_connection"]

    def __init__(self, response_data: dict, connection: Optional[Connection] = None):
        self._data = response_data
        # Mimic original list of process metadata dictionaries
        super().__init__(response_data["processes"])

        self._connection = connection
        self.ext_federation_missing(auto_warn=True)

    def _repr_html_(self):
        federation = self._connection.capabilities().ext_federation_backend_details() if self._connection else None
        return render_component(
            component="processes",
            data=self,
            parameters={
                "show-graph": True,
                "provide-download": False,
                "missing": self.ext_federation_missing(),
                "federation": federation,
            },
        )

    @property
    def links(self) -> List[Link]:
        """Get links related to this resource."""
        return [Link.from_dict(d) for d in self._data.get("links", [])]

    def ext_federation_missing(self, *, auto_warn: bool = False) -> Union[None, List[str]]:
        """
        List of backends IDs (from the federation)
        that were not available during the resource listing request.

        :param auto_warn: whether to automatically log a warning if missing federation components are detected.

        .. seealso:: :ref:`federation-extension`

        .. warning:: this API is experimental and subject to change.
        """
        return federation_extension.get_federation_missing(
            data=self._data, resource_name="process listing", auto_warn=auto_warn
        )


class JobListingResponse(list):
    """
    Container for job metadata listing received
    from a ``GET /jobs`` request.

    .. note::
        This object mimics, for backward compatibility reasons,
        the interface of simple list of job metadata dictionaries (``List[dict]``),
        which was the original return API of
        :py:meth:`~openeo.rest.connection.Connection.list_jobs()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``GET /jobs`` request
    :param connection: optional connection object to use for federation extension

    .. seealso:: :py:meth:`openeo.rest.connection.Connection.list_jobs()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data", "_connection"]

    def __init__(self, response_data: dict, connection: Optional[Connection] = None):
        self._data = response_data
        # Mimic original list of process metadata dictionaries
        super().__init__(response_data["jobs"])

        self._connection = connection
        self.ext_federation_missing(auto_warn=True)

    def _repr_html_(self):
        federation = self._connection.capabilities().ext_federation_backend_details() if self._connection else None
        return render_component(
            component="data-table",
            data=self,
            parameters={
                "columns": "jobs",
                "missing": self.ext_federation_missing(),
                "federation": federation,
            },
        )

    @property
    def links(self) -> List[Link]:
        """Get links related to this resource."""
        return [Link.from_dict(d) for d in self._data.get("links", [])]

    def ext_federation_missing(self, *, auto_warn: bool = False) -> Union[None, List[str]]:
        """
        List of backends IDs (from the federation)
        that were not available during the resource listing request.

        :param auto_warn: whether to automatically log a warning if missing federation components are detected.

        .. seealso:: :ref:`federation-extension`

        .. warning:: this API is experimental and subject to change.
        """
        return federation_extension.get_federation_missing(
            data=self._data, resource_name="job listing", auto_warn=auto_warn
        )


class LogsResponse(list):
    """
    Container for job/service logs as received
    from a ``GET /jobs/{job_id}/logs`` or ``GET /services/{service_id}/logs`` request.

    .. note::
        This object mimics, for backward compatibility reasons,
        the interface of a simple list (``List[LogEntry]``)
        which was the original return API of
        :py:meth:`~openeo.rest.job.BatchJob.logs()`
        and :py:meth:`~openeo.rest.service.Service.logs()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``GET /jobs/{job_id}/logs``
        or ``GET /services/{service_id}/logs`` request.
    :param connection: optional connection object to use for federation extension

    .. seealso:: :py:meth:`~openeo.rest.job.BatchJob.logs()`
        and :py:meth:`~openeo.rest.service.Service.logs()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data", "_connection"]

    def __init__(
        self, response_data: dict, *, log_level: Optional[str] = None, connection: Optional[Connection] = None
    ):
        self._data = response_data

        logs = response_data.get("logs", [])
        # Extra client-side level filtering (in case the back-end does not support that)
        if log_level:

            @functools.lru_cache
            def accept_level(level: str) -> bool:
                return normalize_log_level(level) >= normalize_log_level(log_level)

            if (
                # Backend does not list effective lowest level
                "level" not in response_data
                # Or effective lowest level is still too low
                or not accept_level(response_data["level"])
            ):
                logs = (log for log in logs if accept_level(log.get("level")))
        logs = [LogEntry(log) for log in logs]

        # Mimic original list of process metadata dictionaries
        super().__init__(logs)

        self._connection = connection
        self.ext_federation_missing(auto_warn=True)

    def _repr_html_(self):
        federation = self._connection.capabilities().ext_federation_backend_details() if self._connection else None
        return render_component(
            component="logs",
            data=self,
            parameters={
                "missing": self.ext_federation_missing(),
                "federation": federation,
            },
        )

    @property
    def logs(self) -> List[LogEntry]:
        """Get the log entries."""
        return self

    @property
    def links(self) -> List[Link]:
        """Get links related to this resource."""
        return [Link.from_dict(d) for d in self._data.get("links", [])]

    def ext_federation_missing(self, *, auto_warn: bool = False) -> Union[None, List[str]]:
        """
        List of backends IDs (from the federation)
        that were not available during the resource listing request.

        :param auto_warn: whether to automatically log a warning if missing federation components are detected.

        .. seealso:: :ref:`federation-extension`

        .. warning:: this API is experimental and subject to change.
        """
        return federation_extension.get_federation_missing(
            data=self._data, resource_name="log listing", auto_warn=auto_warn
        )


class ValidationResponse(list):
    """
    Container for process metadata listing received
    from a ``POST /validation`` request.

    .. note::
        This object mimics, for backward compatibility reasons,
        the interface of simple list of validation error dictionaries (``List[dict]``),
        :py:meth:`~openeo.rest.connection.Connection.validate_process_graph()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``POST /validation`` request

    .. seealso:: :py:meth:`openeo.rest.connection.Connection.validate_process_graph()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data"]

    def __init__(self, response_data: dict):
        self._data = response_data
        # Mimic original list of validation error dictionaries
        super().__init__(response_data["errors"])

    def ext_federation_backends(self) -> Union[None, List[str]]:
        """
        Get "federation:backends" value from validation response.

        .. seealso:: :ref:`federation-extension`

        .. warning:: this API is experimental and subject to change.
        """
        return federation_extension.get_federation_backends(data=self._data)

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union

from openeo.internal.jupyter import render_component
from openeo.rest.models.federation_extension import FederationExtension


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
        This object mimics a simple list of collection metadata dictionaries,
        which was the original return API of
        :py:meth:`~openeo.rest.connection.Connection.list_collections()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``GET /collections`` request

    .. seealso:: :py:meth:`openeo.rest.connection.Connection.list_collections()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data"]

    def __init__(self, response_data: dict, *, warn_on_federation_missing: bool = True):
        self._data = response_data
        # Mimic original list of collection metadata dictionaries
        super().__init__(response_data["collections"])
        if warn_on_federation_missing:
            self.ext_federation.warn_on_missing(resource_name="collection listing")

    def _repr_html_(self):
        return render_component(component="collections", data=self)

    @property
    def links(self) -> List[Link]:
        """Get links related to this resource."""
        return [Link.from_dict(d) for d in self._data.get("links", [])]

    @property
    def ext_federation(self) -> FederationExtension:
        """Accessor for federation extension data related to this resource."""
        return FederationExtension(self._data)


class ProcessListingResponse(list):
    """
    Container for process metadata listing received
    from a ``GET /processes`` request.

    .. note::
        This object mimics a simple list of collection metadata dictionaries,
        which was the original return API of
        :py:meth:`~openeo.rest.connection.Connection.list_processes()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``GET /processes`` request

    .. seealso:: :py:meth:`openeo.rest.connection.Connection.list_processes()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data"]

    def __init__(self, response_data: dict, *, warn_on_federation_missing: bool = True):
        self._data = response_data
        # Mimic original list of process metadata dictionaries
        super().__init__(response_data["processes"])
        if warn_on_federation_missing:
            self.ext_federation.warn_on_missing(resource_name="process listing")

    def _repr_html_(self):
        return render_component(
            component="processes", data=self, parameters={"show-graph": True, "provide-download": False}
        )

    @property
    def links(self) -> List[Link]:
        """Get links related to this resource."""
        return [Link.from_dict(d) for d in self._data.get("links", [])]

    @property
    def ext_federation(self) -> FederationExtension:
        """Accessor for federation extension data related to this resource."""
        return FederationExtension(self._data)


class JobListingResponse(list):
    """
    Container for job metadata listing received
    from a ``GET /jobs`` request.

    .. note::
        This object mimics a simple ``List[dict]`` with job metadata,
        which was the original return API of
        :py:meth:`~openeo.rest.connection.Connection.list_jobs()`,
        but now also provides methods/properties to access additional response data.

    :param response_data: response data from a ``GET /jobs`` request

    .. seealso:: :py:meth:`openeo.rest.connection.Connection.list_jobs()`

    .. versionadded:: 0.38.0
    """

    __slots__ = ["_data"]

    def __init__(self, response_data: dict, *, warn_on_federation_missing: bool = True):
        self._data = response_data
        # Mimic original list of process metadata dictionaries
        super().__init__(response_data["jobs"])
        if warn_on_federation_missing:
            self.ext_federation.warn_on_missing(resource_name="job listing")

    def _repr_html_(self):
        return render_component(component="data-table", data=self, parameters={"columns": "jobs"})

    @property
    def links(self) -> List[Link]:
        """Get links related to this resource."""
        return [Link.from_dict(d) for d in self._data.get("links", [])]

    @property
    def ext_federation(self) -> FederationExtension:
        """Accessor for federation extension data related to this resource."""
        return FederationExtension(self._data)

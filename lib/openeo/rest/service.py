from __future__ import annotations

import typing
from typing import List, Optional, Union

from openeo.api.logs import LogEntry, log_level_name
from openeo.internal.jupyter import VisualDict, VisualList

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection


class Service:
    """Represents a secondary web service in openeo."""

    def __init__(self, service_id: str, connection: Connection):
        # Unique identifier of the secondary web service (string)
        self.service_id = service_id
        self.connection = connection

    def __repr__(self):
        return '<{c} service_id={i!r}>'.format(c=self.__class__.__name__, i=self.service_id)

    def _repr_html_(self):
        data = self.describe_service()
        currency = self.connection.capabilities().currency()
        return VisualDict('service', data = data, parameters = {'currency': currency})

    def describe_service(self):
        """ Get all information about a secondary web service."""
        # GET /services/{service_id}
        return self.connection.get("/services/{}".format(self.service_id), expected_status=200).json()

    def update_service(self, process_graph=None, title=None, description=None, enabled=None, configuration=None, plan=None, budget=None, additional=None):
        """ Update a secondary web service."""
        # PATCH /services/{service_id}
        raise NotImplementedError

    def delete_service(self):
        """ Delete a secondary web service."""
        # DELETE /services/{service_id}
        self.connection.delete("/services/{}".format(self.service_id), expected_status=204)

    def logs(
        self, offset: Optional[str] = None, level: Optional[Union[str, int]] = None
    ) -> List[LogEntry]:
        """Retrieve service logs."""
        url = f"/service/{self.service_id}/logs"
        params = {}
        if offset is not None:
            params["offset"] = offset
        if level is not None:
            params["level"] = log_level_name(level)
        resp = self.connection.get(url, params=params, expected_status=200)
        logs = resp.json()["logs"]
        entries = [LogEntry(log) for log in logs]
        return VisualList("logs", data=entries)

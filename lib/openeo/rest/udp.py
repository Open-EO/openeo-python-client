from __future__ import annotations

import typing
from pathlib import Path
from typing import List, Optional, Union

from openeo.api.process import Parameter
from openeo.internal.graph_building import FlatGraphableMixin, as_flat_graph
from openeo.internal.jupyter import render_component
from openeo.internal.processes.builder import ProcessBuilderBase
from openeo.internal.warnings import deprecated
from openeo.util import dict_no_none

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection


def build_process_dict(
    process_graph: Union[dict, FlatGraphableMixin, Path, List[FlatGraphableMixin]],
    process_id: Optional[str] = None,
    summary: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[List[Union[Parameter, dict]]] = None,
    returns: Optional[dict] = None,
    categories: Optional[List[str]] = None,
    examples: Optional[List[dict]] = None,
    links: Optional[List[dict]] = None,
) -> dict:
    """
    Build a dictionary describing a process with metadaa (`process_graph`, `parameters`, `description`, ...)

    :param process_graph: dict or builder representing a process graph
    :param process_id: identifier of the process
    :param summary: short summary of what the process does
    :param description: detailed description
    :param parameters: list of process parameters (which have name, schema, default value, ...)
    :param returns: description and schema of what the process returns
    :param categories: list of categories
    :param examples: list of examples, may be used for unit tests
    :param links: list of links related to the process
    :return: dictionary in openEO "process graph with metadata" format
    """
    process = dict_no_none(
        process_graph=as_flat_graph(process_graph),
        id=process_id,
        summary=summary,
        description=description,
        returns=returns,
        categories=categories,
        examples=examples,
        links=links
    )
    if parameters is not None:
        process["parameters"] = [
            (p if isinstance(p, Parameter) else Parameter(**p)).to_dict()
            for p in parameters
        ]
    return process


class RESTUserDefinedProcess:
    """
    Wrapper for a user-defined process stored (or to be stored) on an openEO back-end
    """

    def __init__(self, user_defined_process_id: str, connection: Connection):
        self.user_defined_process_id = user_defined_process_id
        self._connection = connection
        self._connection.assert_user_defined_process_support()

    def _repr_html_(self):
        process = self.describe()
        return render_component('process', data=process, parameters = {'show-graph': True, 'provide-download': False})

    def store(
        self,
        process_graph: Union[dict, FlatGraphableMixin],
        parameters: Optional[List[Union[Parameter, dict]]] = None,
        public: bool = False,
        summary: Optional[str] = None,
        description: Optional[str] = None,
        returns: Optional[dict] = None,
        categories: Optional[List[str]] = None,
        examples: Optional[List[dict]] = None,
        links: Optional[List[dict]] = None,
    ):
        """Store a process graph and its metadata on the backend as a user-defined process"""
        process = build_process_dict(
            process_graph=process_graph, parameters=parameters,
            summary=summary, description=description, returns=returns,
            categories=categories, examples=examples, links=links,
        )

        # TODO: this "public" flag is not standardized yet EP-3609, https://github.com/Open-EO/openeo-api/issues/310
        process["public"] = public

        self._connection._preflight_validation(pg_with_metadata={"process": process})
        self._connection.put(
            path="/process_graphs/{}".format(self.user_defined_process_id), json=process, expected_status=200
        )

    @deprecated(
        "Use `store` instead. Method `update` is misleading: OpenEO API does not provide (partial) updates"
        " of user-defined processes, only fully overwriting 'store' operations.",
        version="0.4.11")
    def update(
            self, process_graph: Union[dict, ProcessBuilderBase], parameters: List[Union[Parameter, dict]] = None,
            public: bool = False, summary: str = None, description: str = None
    ):
        self.store(process_graph=process_graph, parameters=parameters, public=public, summary=summary,
                   description=description)

    def describe(self) -> dict:
        """Get metadata of this user-defined process."""
        # TODO: parse the "parameters" to Parameter objects?
        return self._connection.get(path="/process_graphs/{}".format(self.user_defined_process_id)).json()

    def delete(self) -> None:
        """Remove user-defined process from back-end"""
        self._connection.delete(path="/process_graphs/{}".format(self.user_defined_process_id), expected_status=204)

    def validate(self) -> None:
        raise NotImplementedError

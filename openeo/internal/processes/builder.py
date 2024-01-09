import inspect
import logging
import warnings
from typing import Any, Callable, Dict, List, Optional, Union

from openeo.internal.graph_building import FlatGraphableMixin, PGNode, _FromNodeMixin
from openeo.rest import OpenEoClientException

UNSET = object()
_log = logging.getLogger(__name__)


def _to_pgnode_data(value: Any) -> Union[PGNode, dict, Any]:
    """Convert given value to valid process graph material"""
    if isinstance(value, ProcessBuilderBase):
        return value.pgnode
    elif isinstance(value, list):
        return [_to_pgnode_data(item) for item in value]
    elif isinstance(value, Callable):
        pg = convert_callable_to_pgnode(value)
        return PGNode.to_process_graph_argument(pg)
    else:
        # Fallback: assume value is valid process graph material already.
        return value


class ProcessBuilderBase(_FromNodeMixin, FlatGraphableMixin):
    """
    Base implementation of a builder pattern that allows constructing process graphs
    by calling functions.
    """

    # TODO: can this implementation be merged with PGNode directly?

    def __init__(self, pgnode: Union[PGNode, dict, list]):
        self.pgnode = pgnode

    @classmethod
    def process(cls, process_id: str, arguments: dict = None, namespace: Union[str, None] = None, **kwargs):
        """
        Apply process, using given arguments

        :param process_id: process id of the process.
        :param arguments: argument dictionary for the process.
        :param namespace: process namespace (only necessary to specify for non-predefined or non-user-defined processes)
        :return: new ProcessBuilder instance
        """
        arguments = {**(arguments or {}), **kwargs}
        arguments = {k: _to_pgnode_data(v) for k, v in arguments.items() if v is not UNSET}
        return cls(PGNode(process_id=process_id, arguments=arguments, namespace=namespace))

    def flat_graph(self) -> Dict[str, dict]:
        """Get the process graph in internal flat dict representation."""
        return self.pgnode.flat_graph()

    def from_node(self) -> PGNode:
        # _FromNodeMixin API
        return self.pgnode


def get_parameter_names(process: Callable) -> List[str]:
    """Get argument (aka parameter) names of given function/callable."""
    signature = inspect.signature(process)
    return [
        p.name
        for p in signature.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]


def convert_callable_to_pgnode(callback: Callable, parent_parameters: Optional[List[str]] = None) -> PGNode:
    """
    Convert given process callback to a PGNode.

        >>> result = convert_callable_to_pgnode(lambda x: x + 5)
        >>> assert isinstance(result, PGNode)
        >>> result.flat_graph()
        {"add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "x"}, "y": 5}, "result": True}}

    """
    # TODO: eliminate local import (due to circular dependency)?
    from openeo.processes import ProcessBuilder

    process_params = get_parameter_names(callback)
    if parent_parameters is None:
        # Due to lack of parent parameter information,
        # we blindly use all callback's argument names as parameter names
        # TODO #426: Instead of guessing: extract expected parent_parameters, e.g. based on parent process_id?
        message = f"Blindly using callback parameter names from {callback!r} argument names: {process_params!r}"
        if tuple(process_params) not in {(), ("x",), ("data",), ("x", "y")}:
            warnings.warn(message)
        else:
            _log.info(message)
        kwargs = {p: ProcessBuilder({"from_parameter": p}) for p in process_params}
    elif parent_parameters == ["x", "y"] and (len(process_params) == 1 or process_params[:1] == ["data"]):
        # Special case: wrap all parent parameters in an array
        kwargs = {process_params[0]: ProcessBuilder([{"from_parameter": p} for p in parent_parameters])}
    else:
        # Check for direct correspondence between callback arguments and parent parameters (or subset thereof).
        common = set(parent_parameters).intersection(process_params)
        if common:
            kwargs = {p: ProcessBuilder({"from_parameter": p}) for p in common}
        elif min(len(parent_parameters), len(process_params)) == 0:
            kwargs = {}
        elif min(len(parent_parameters), len(process_params)) == 1:
            # Fallback for common case of just one callback argument (pass the main parameter),
            # or one parent parameter (just pass that one)
            kwargs = {process_params[0]: ProcessBuilder({"from_parameter": parent_parameters[0]})}
        else:
            raise OpenEoClientException(
                f"Callback argument mismatch: expected (prefix of) {parent_parameters}, but found found {process_params!r}"
            )

    # "Evaluate" the callback, which should give a ProcessBuilder again to extract pgnode from
    result = callback(**kwargs)
    if not isinstance(result, ProcessBuilderBase):
        raise OpenEoClientException(
            f"Callback {callback} did not evaluate to ProcessBuilderBase. Got {result!r} instead"
        )
    return result.pgnode

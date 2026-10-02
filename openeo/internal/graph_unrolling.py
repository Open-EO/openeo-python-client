from __future__ import annotations

import copy
from typing import Callable, Dict, Iterable, Mapping, Optional, Tuple


class ProcessGraphUnrollError(ValueError):
    """Raised when a process graph can not be unrolled safely."""


ProcessDefinitionResolver = Callable[[Mapping], Optional[Mapping]]


class ProcessGraphUnroller:
    """Inline user-defined process definitions into a flat process graph."""

    def __init__(self, *, resolve_process_definition: ProcessDefinitionResolver):
        self._resolve_process_definition = resolve_process_definition

    def unroll(self, process_graph: Mapping[str, Mapping]) -> Dict[str, dict]:
        return self._unroll_graph(copy.deepcopy(dict(process_graph)), stack=())

    def _unroll_graph(
        self, process_graph: Dict[str, dict], *, stack: Tuple[Tuple[str, Optional[str]], ...]
    ) -> Dict[str, dict]:
        process_graph = self._unroll_callback_graphs(process_graph=process_graph, stack=stack)

        while True:
            expandable = next(
                (
                    (node_id, node, definition)
                    for node_id, node in process_graph.items()
                    if (definition := self._resolve_process_definition(node)) is not None
                ),
                None,
            )
            if expandable is None:
                return process_graph

            node_id, node, definition = expandable
            process_key = (str(node.get("process_id")), node.get("namespace"))
            if process_key in stack:
                chain = " -> ".join(pid for pid, _ in (*stack, process_key))
                raise ProcessGraphUnrollError(f"Recursive process definition detected: {chain}")

            inlined = self._instantiate_definition(
                definition=definition,
                arguments=node.get("arguments", {}),
                invocation_id=node_id,
                used_node_ids=process_graph,
            )
            inlined = self._unroll_graph(inlined, stack=(*stack, process_key))
            result_id = self._get_result_node_id(inlined)

            if node.get("result"):
                inlined[result_id]["result"] = True
            else:
                inlined[result_id].pop("result", None)

            process_graph = self._replace_node(
                process_graph=process_graph,
                node_id=node_id,
                replacement=inlined,
                replacement_result_id=result_id,
            )
            process_graph = self._unroll_callback_graphs(process_graph=process_graph, stack=stack)

    def _instantiate_definition(
        self,
        *,
        definition: Mapping,
        arguments: Mapping,
        invocation_id: str,
        used_node_ids: Iterable[str],
    ) -> Dict[str, dict]:
        process_graph = definition.get("process_graph")
        if not isinstance(process_graph, Mapping) or not process_graph:
            raise ProcessGraphUnrollError(
                f"Process definition {definition.get('id', invocation_id)!r} has no process graph"
            )

        bindings = self._build_parameter_bindings(definition=definition, arguments=arguments)
        node_id_map = self._build_node_id_map(
            inner_node_ids=process_graph,
            invocation_id=invocation_id,
            used_node_ids=used_node_ids,
        )

        instantiated = {}
        for inner_id, inner_node in copy.deepcopy(dict(process_graph)).items():
            inner_node = self._rewrite_references(inner_node, node_id_map)
            inner_node = self._bind_parameters(inner_node, bindings)
            instantiated[node_id_map[inner_id]] = inner_node
        return instantiated

    @staticmethod
    def _build_parameter_bindings(*, definition: Mapping, arguments: Mapping) -> Dict[str, object]:
        parameters = definition.get("parameters", [])
        if not isinstance(parameters, list):
            raise ProcessGraphUnrollError(f"Invalid parameters in process definition {definition.get('id')!r}")

        bindings = {}
        missing = []
        for parameter in parameters:
            name = parameter.get("name")
            if not isinstance(name, str):
                raise ProcessGraphUnrollError(f"Invalid parameter in process definition {definition.get('id')!r}")
            if name in arguments:
                bindings[name] = copy.deepcopy(arguments[name])
            elif "default" in parameter:
                bindings[name] = copy.deepcopy(parameter["default"])
            elif parameter.get("optional", False):
                bindings[name] = None
            else:
                missing.append(name)

        if missing:
            raise ProcessGraphUnrollError(
                f"Missing required arguments {sorted(missing)} for process {definition.get('id')!r}"
            )
        return bindings

    @staticmethod
    def _build_node_id_map(
        *, inner_node_ids: Iterable[str], invocation_id: str, used_node_ids: Iterable[str]
    ) -> Dict[str, str]:
        used = set(used_node_ids)
        result = {}
        for inner_id in inner_node_ids:
            base = f"{invocation_id}_{inner_id}"
            candidate = base
            index = 2
            while candidate in used:
                candidate = f"{base}_{index}"
                index += 1
            result[inner_id] = candidate
            used.add(candidate)
        return result

    def _unroll_callback_graphs(
        self, *, process_graph: Dict[str, dict], stack: Tuple[Tuple[str, Optional[str]], ...]
    ) -> Dict[str, dict]:
        def visit(value):
            if isinstance(value, dict):
                return {
                    key: (
                        self._unroll_graph(copy.deepcopy(child), stack=stack)
                        if key == "process_graph" and isinstance(child, dict)
                        else visit(child)
                    )
                    for key, child in value.items()
                }
            if isinstance(value, list):
                return [visit(child) for child in value]
            return value

        return {node_id: visit(node) for node_id, node in process_graph.items()}

    @staticmethod
    def _rewrite_references(value, node_id_map: Mapping[str, str]):
        if isinstance(value, dict):
            if set(value) == {"from_node"} and value["from_node"] in node_id_map:
                return {"from_node": node_id_map[value["from_node"]]}
            return {
                key: child if key == "process_graph" else ProcessGraphUnroller._rewrite_references(child, node_id_map)
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [ProcessGraphUnroller._rewrite_references(child, node_id_map) for child in value]
        return value

    @staticmethod
    def _bind_parameters(value, bindings: Mapping[str, object]):
        if isinstance(value, dict):
            if set(value) == {"from_parameter"}:
                parameter = value["from_parameter"]
                if parameter not in bindings:
                    raise ProcessGraphUnrollError(f"Unknown process parameter {parameter!r}")
                return copy.deepcopy(bindings[parameter])
            return {
                key: child if key == "process_graph" else ProcessGraphUnroller._bind_parameters(child, bindings)
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [ProcessGraphUnroller._bind_parameters(child, bindings) for child in value]
        return value

    @staticmethod
    def _get_result_node_id(process_graph: Mapping[str, Mapping]) -> str:
        result_nodes = [node_id for node_id, node in process_graph.items() if node.get("result") is True]
        if len(result_nodes) != 1:
            raise ProcessGraphUnrollError(f"Expected one result node, found {len(result_nodes)}")
        return result_nodes[0]

    @staticmethod
    def _replace_node(
        *,
        process_graph: Dict[str, dict],
        node_id: str,
        replacement: Dict[str, dict],
        replacement_result_id: str,
    ) -> Dict[str, dict]:
        def replace_reference(value):
            if isinstance(value, dict):
                if value == {"from_node": node_id}:
                    return {"from_node": replacement_result_id}
                return {
                    key: child if key == "process_graph" else replace_reference(child) for key, child in value.items()
                }
            if isinstance(value, list):
                return [replace_reference(child) for child in value]
            return value

        result = {}
        for current_id, current_node in process_graph.items():
            if current_id == node_id:
                result.update(replacement)
            else:
                result[current_id] = replace_reference(current_node)
        return result

import pytest

from openeo.internal.graph_unrolling import (
    ProcessGraphUnroller,
    ProcessGraphUnrollError,
)


def test_unroll_process_with_parameters_and_default():
    definitions = {
        "increment": {
            "id": "increment",
            "parameters": [
                {"name": "data"},
                {"name": "amount", "default": 1},
            ],
            "process_graph": {
                "add1": {
                    "process_id": "add",
                    "arguments": {
                        "x": {"from_parameter": "data"},
                        "y": {"from_parameter": "amount"},
                    },
                    "result": True,
                }
            },
        }
    }
    unroller = ProcessGraphUnroller(resolve_process_definition=lambda node: definitions.get(node["process_id"]))
    graph = {
        "load1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
        "increment1": {
            "process_id": "increment",
            "arguments": {"data": {"from_node": "load1"}},
        },
        "save1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "increment1"}, "format": "GTiff"},
            "result": True,
        },
    }

    result = unroller.unroll(graph)

    assert result == {
        "load1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
        "increment1_add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "load1"}, "y": 1},
        },
        "save1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "increment1_add1"}, "format": "GTiff"},
            "result": True,
        },
    }
    assert graph["increment1"]["process_id"] == "increment"


def test_unroll_nested_processes():
    definitions = {
        "increment": {
            "id": "increment",
            "parameters": [{"name": "data"}],
            "process_graph": {
                "add1": {
                    "process_id": "add",
                    "arguments": {"x": {"from_parameter": "data"}, "y": 1},
                    "result": True,
                }
            },
        },
        "increment_twice": {
            "id": "increment_twice",
            "parameters": [{"name": "data"}],
            "process_graph": {
                "first": {
                    "process_id": "increment",
                    "arguments": {"data": {"from_parameter": "data"}},
                },
                "second": {
                    "process_id": "increment",
                    "arguments": {"data": {"from_node": "first"}},
                    "result": True,
                },
            },
        },
    }
    unroller = ProcessGraphUnroller(resolve_process_definition=lambda node: definitions.get(node["process_id"]))

    result = unroller.unroll(
        {
            "twice1": {
                "process_id": "increment_twice",
                "arguments": {"data": 5},
                "result": True,
            }
        }
    )

    assert result == {
        "twice1_first_add1": {"process_id": "add", "arguments": {"x": 5, "y": 1}},
        "twice1_second_add1": {
            "process_id": "add",
            "arguments": {"x": {"from_node": "twice1_first_add1"}, "y": 1},
            "result": True,
        },
    }


def test_unroll_recursive_process_fails():
    definition = {
        "id": "recursive",
        "parameters": [],
        "process_graph": {"again": {"process_id": "recursive", "arguments": {}, "result": True}},
    }
    unroller = ProcessGraphUnroller(
        resolve_process_definition=lambda node: definition if node["process_id"] == "recursive" else None
    )

    with pytest.raises(ProcessGraphUnrollError, match="Recursive process definition detected: recursive -> recursive"):
        unroller.unroll({"recursive1": {"process_id": "recursive", "arguments": {}, "result": True}})


def test_unroll_process_in_callback_scope():
    increment = {
        "id": "increment",
        "parameters": [{"name": "data"}],
        "process_graph": {
            "add1": {
                "process_id": "add",
                "arguments": {"x": {"from_parameter": "data"}, "y": 1},
                "result": True,
            }
        },
    }
    unroller = ProcessGraphUnroller(
        resolve_process_definition=lambda node: increment if node["process_id"] == "increment" else None
    )

    result = unroller.unroll(
        {
            "apply1": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "load1"},
                    "process": {
                        "process_graph": {
                            "increment1": {
                                "process_id": "increment",
                                "arguments": {"data": {"from_parameter": "x"}},
                                "result": True,
                            }
                        }
                    },
                },
                "result": True,
            },
            "load1": {"process_id": "load_collection", "arguments": {"id": "S2"}},
        }
    )

    callback = result["apply1"]["arguments"]["process"]["process_graph"]
    assert callback == {
        "increment1_add1": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "x"}, "y": 1},
            "result": True,
        }
    }


def test_unroll_missing_required_argument_fails():
    definition = {
        "id": "increment",
        "parameters": [{"name": "data"}],
        "process_graph": {"add1": {"process_id": "add", "arguments": {}, "result": True}},
    }
    unroller = ProcessGraphUnroller(resolve_process_definition=lambda node: definition)

    with pytest.raises(ProcessGraphUnrollError, match=r"Missing required arguments \['data'\]"):
        unroller.unroll({"increment1": {"process_id": "increment", "arguments": {}, "result": True}})

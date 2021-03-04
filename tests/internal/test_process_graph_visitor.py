from unittest.mock import MagicMock, call, ANY

import pytest

from openeo.internal.process_graph_visitor import ProcessGraphVisitor


def test_visit_node():
    node = {
        "process_id": "cos",
        "arguments": {"x": {"from_argument": "data"}}
    }
    visitor = ProcessGraphVisitor()
    visitor.enterProcess = MagicMock()
    visitor.enterArgument = MagicMock()
    visitor.accept_node(node)

    assert visitor.enterProcess.call_args_list == [
        call(process_id="cos", arguments={"x": {"from_argument": "data"}}, namespace=None)
    ]
    assert visitor.enterArgument.call_args_list == [call(argument_id="x", value={"from_argument": "data"})]


def test_visit_node_namespaced():
    node = {
        "process_id": "cos",
        "namespace": "math",
        "arguments": {"x": {"from_argument": "data"}}
    }
    visitor = ProcessGraphVisitor()
    visitor.enterProcess = MagicMock()
    visitor.enterArgument = MagicMock()
    visitor.accept_node(node)

    assert visitor.enterProcess.call_args_list == [
        call(process_id="cos", arguments={"x": {"from_argument": "data"}}, namespace="math")
    ]
    assert visitor.enterArgument.call_args_list == [call(argument_id="x", value={"from_argument": "data"})]


def test_visit_nodes():
    graph = {
        "abs": {
            "process_id": "abs",
            "arguments": {
                "data": {
                    "from_argument": "data"
                }
            },
        },
        "cos": {
            "process_id": "cos",
            "arguments": {
                "data": {
                    "from_node": "abs"
                },
                "data2": {
                    "from_parameter": "x"
                }
            },
            "result": True
        }
    }
    visitor = ProcessGraphVisitor()
    visitor.leaveProcess = MagicMock()
    visitor.enterArgument = MagicMock()
    visitor.from_parameter = MagicMock()

    visitor.accept_process_graph(graph)

    assert visitor.leaveProcess.call_args_list == [
        call(process_id="abs", arguments=ANY, namespace=None),
        call(process_id="cos", arguments=ANY, namespace=None),
    ]
    assert visitor.enterArgument.call_args_list == [
        call(argument_id="data", value=ANY),
        call(argument_id="data", value={"from_argument": "data"}),
        call(argument_id='data2', value={'from_parameter': 'x'})
    ]
    assert visitor.from_parameter.call_args_list == [
        call("x")
    ]


def test_visit_nodes_array():
    graph = {
        "abs": {
            "arguments": {
                "data": [
                    {"from_argument": "data"},
                    10.0
                ]
            },
            "process_id": "abs"
        },
        "cos": {
            "arguments": {
                "data": {
                    "from_node": "abs"
                }
            },
            "process_id": "cos",
            "result": True
        }
    }

    visitor = ProcessGraphVisitor()
    visitor.leaveProcess = MagicMock()
    visitor.enterArgument = MagicMock()
    visitor.enterArray = MagicMock()

    visitor.accept_process_graph(graph)
    assert visitor.leaveProcess.call_args_list == [
        call(process_id='abs', arguments=ANY, namespace=None),
        call(process_id='cos', arguments=ANY, namespace=None)
    ]
    assert visitor.enterArgument.call_args_list == [
        call(argument_id="data", value=ANY)
    ]
    assert visitor.enterArray.call_args_list == [
        call(argument_id="data")
    ]


def test_visit_array_with_dereferenced_nodes():
    graph = {
        'arrayelement1': {
            'arguments': {'data': {'from_argument': 'data'}, 'index': 2},
            'process_id': 'array_element',
            'result': False
        },
        'product1': {
            'process_id': 'product',
            'arguments': {'data': [{'from_node': 'arrayelement1'}, -1]},
            'result': True
        }
    }
    top = ProcessGraphVisitor.dereference_from_node_arguments(graph)
    dereferenced = graph[top]
    assert dereferenced["arguments"]["data"][0]["arguments"]["data"]["from_argument"] == "data"

    visitor = ProcessGraphVisitor()
    visitor.leaveProcess = MagicMock()
    visitor.enterArgument = MagicMock()
    visitor.enterArray = MagicMock()
    visitor.arrayElementDone = MagicMock()
    visitor.constantArrayElement = MagicMock()

    visitor.accept_node(dereferenced)
    assert visitor.leaveProcess.call_args_list == [
        call(process_id='array_element', arguments=ANY, namespace=None),
        call(process_id='product', arguments=ANY, namespace=None)
    ]
    assert visitor.enterArgument.call_args_list == [
        call(argument_id="data", value={'from_argument': 'data'})
    ]
    assert visitor.enterArray.call_args_list == [
        call(argument_id="data")
    ]
    assert visitor.arrayElementDone.call_args_list == [
        call({
            "process_id": "array_element",
            "arguments": {"data": {"from_argument": "data"}, "index": 2},
            "result": False
        })
    ]
    assert visitor.constantArrayElement.call_args_list == [
        call(-1)
    ]


def test_dereference_basic():
    graph = {
        "node1": {},
        "node2": {
            "arguments": {
                "data1": {
                    "from_node": "node1"
                },
                "data2": {
                    "from_node": "node3"
                }
            },
            "result": True
        },
        "node3": {
            "arguments": {
                "data": {
                    "from_node": "node4"
                }
            }
        },
        "node4": {}
    }
    result = ProcessGraphVisitor.dereference_from_node_arguments(graph)

    assert result == "node2"
    assert graph["node1"] == graph["node2"]["arguments"]["data1"]["node"]
    assert graph["node3"] == graph["node2"]["arguments"]["data2"]["node"]
    assert graph["node4"] == graph["node3"]["arguments"]["data"]["node"]
    assert graph == {
        "node1": {},
        "node2": {
            "arguments": {
                "data1": {"from_node": "node1", "node": {}},
                "data2": {"from_node": "node3", "node": {
                    "arguments": {
                        "data": {"from_node": "node4", "node": {}},
                    }
                }},
            },
            "result": True
        },
        "node3": {
            "arguments": {
                "data": {"from_node": "node4", "node": {}},
            }
        },
        "node4": {}

    }


def test_dereference_list_arg():
    graph = {
        "start": {"process_id": "constant", "arguments": {"x": "2020-02-02"}},
        "end": {"process_id": "constant", "arguments": {"x": "2020-03-03"}},
        "temporal": {
            "process_id": "filter_temporal",
            "arguments": {
                "extent": [{"from_node": "start"}, {"from_node": "end"}],
            },
            "result": True,
        },
    }
    result = ProcessGraphVisitor.dereference_from_node_arguments(graph)
    assert result == "temporal"
    assert graph == {
        "start": {"process_id": "constant", "arguments": {"x": "2020-02-02"}},
        "end": {"process_id": "constant", "arguments": {"x": "2020-03-03"}},
        "temporal": {
            "process_id": "filter_temporal",
            "arguments": {
                "extent": [
                    {"process_id": "constant", "arguments": {"x": "2020-02-02"}},
                    {"process_id": "constant", "arguments": {"x": "2020-03-03"}},
                ],
            },
            "result": True,
        },
    }


def test_dereference_dict_arg():
    graph = {
        "west": {"process_id": "add", "arguments": {"x": 1, "y": 1}},
        "east": {"process_id": "add", "arguments": {"x": 2, "y": 3}},
        "bbox": {
            "process_id": "filter_bbox",
            "arguments": {
                "extent": {
                    "west": {"from_node": "west"},
                    "east": {"from_node": "east"},
                }
            },
            "result": True,
        }
    }
    result = ProcessGraphVisitor.dereference_from_node_arguments(graph)
    assert result == "bbox"
    assert graph == {
        "west": {"process_id": "add", "arguments": {"x": 1, "y": 1}},
        "east": {"process_id": "add", "arguments": {"x": 2, "y": 3}},
        "bbox": {
            "process_id": "filter_bbox",
            "arguments": {
                "extent": {
                    "west": {
                        "from_node": "west",
                        "node": {"process_id": "add", "arguments": {"x": 1, "y": 1}},
                    },
                    "east": {
                        "from_node": "east",
                        "node": {"process_id": "add", "arguments": {"x": 2, "y": 3}},
                    },
                }
            },
            "result": True,
        }
    }


def test_dereference_no_result_node():
    with pytest.raises(ValueError, match="does not contain a result node"):
        ProcessGraphVisitor.dereference_from_node_arguments({
            "node1": {},
            "node2": {}
        })


def test_dereference_multiple_result_node():
    with pytest.raises(ValueError, match="Multiple result nodes"):
        ProcessGraphVisitor.dereference_from_node_arguments({
            "node1": {"result": True},
            "node2": {"result": True}
        })


def test_dereference_invalid_node():
    graph = {
        "node1": {},
        "node2": {
            "arguments": {
                "data": {
                    "from_node": "node3"
                }
            },
            "result": True
        }
    }
    with pytest.raises(ValueError, match="not in process graph"):
        ProcessGraphVisitor.dereference_from_node_arguments(graph)


def test_dereference_cycle():
    graph = {
        "node1": {
            "arguments": {
                "data": {"from_node": "node2"},
            },
            "result": True
        },
        "node2": {
            "arguments": {
                "data": {"from_node": "node1"},
            }
        }
    }
    ProcessGraphVisitor.dereference_from_node_arguments(graph)
    assert graph["node1"]["arguments"]["data"]["node"] is graph["node2"]
    assert graph["node2"]["arguments"]["data"]["node"] is graph["node1"]

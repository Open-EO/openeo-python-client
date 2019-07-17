from unittest import TestCase
from unittest.mock import MagicMock, call, ANY

import pytest

from openeo.internal.process_graph_visitor import ProcessGraphVisitor


class GraphVisitorTest(TestCase):

    def test_visit_nodes(self):
        graph = {
                    "abs":{
                        "arguments":{
                            "data": {
                                "from_argument": "data"
                            }
                        },
                        "process_id":"abs"
                    },
                    "cos": {
                        "arguments":{
                            "data": {
                                "from_node": "abs"
                            }
                        },
                        "process_id": "cos",
                        "result": True
                    }
                }
        original = ProcessGraphVisitor()

        leaveProcess = MagicMock(original.leaveProcess)
        original.leaveProcess = leaveProcess

        enterArgument = MagicMock(original.enterArgument)
        original.enterArgument = enterArgument

        original.accept_process_graph(graph)
        assert leaveProcess.call_count == 2
        leaveProcess.assert_has_calls([
            call('abs',ANY),
            call('cos', ANY)
        ])

        assert enterArgument.call_count == 2
        enterArgument.assert_has_calls([
            call('data', ANY),
            call('data', ANY)
        ])

        print(leaveProcess)

    def test_visit_nodes_array(self):
        graph = {
                    "abs":{
                        "arguments":{
                            "data": [{
                                "from_argument": "data"
                            },
                            10.0
                            ]
                        },
                        "process_id":"abs"
                    },
                    "cos": {
                        "arguments":{
                            "data": {
                                "from_node": "abs"
                            }
                        },
                        "process_id": "cos",
                        "result": True
                    }
                }
        original = ProcessGraphVisitor()

        leaveProcess = MagicMock(original.leaveProcess)
        original.leaveProcess = leaveProcess

        enterArgument = MagicMock(original.enterArgument)
        original.enterArgument = enterArgument

        arrayStart = MagicMock(original.enterArray)
        original.enterArray = arrayStart

        original.accept_process_graph(graph)
        self.assertEqual(2, leaveProcess.call_count)
        leaveProcess.assert_has_calls([
            call('abs',ANY),
            call('cos', ANY)
        ])

        self.assertEqual(1, enterArgument.call_count)
        enterArgument.assert_has_calls([
            call('data', ANY)
        ])

        self.assertEqual(1, arrayStart.call_count)
        arrayStart.assert_has_calls([
            call('data')
        ])

        print(leaveProcess)


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


def test_dereference_no_result_node():
    with pytest.raises(ValueError, match="does not contain a result node"):
        ProcessGraphVisitor.dereference_from_node_arguments({
            "node1": {},
            "node2": {}
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


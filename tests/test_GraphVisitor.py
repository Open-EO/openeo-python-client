from unittest import TestCase
from unittest.mock import MagicMock,call,ANY

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

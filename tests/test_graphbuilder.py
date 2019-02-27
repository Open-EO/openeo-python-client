from unittest import TestCase
from openeo.graphbuilder import GraphBuilder


class GraphBuilderTest(TestCase):

    def test_create_empty(self):
        builder = GraphBuilder()
        builder.process("sum",{})
        self.assertEqual(1,len(builder.processes))


    def test_create_from_existing(self):
        graph = {
            "sum_01": {
                "arguments": {
                    "data1": {
                        "from_node": "node1"
                    },
                    "data2": {
                        "from_node": "node3"
                    }
                },
                "process_id":"sum",
                "result": True
            },
            "sum_02": {
                "arguments": {
                    "data": {
                        "from_node": "node4"
                    }
                },
                "process_id": "sum",
            }
        }

        builder = GraphBuilder(graph)

        print(builder.processes)
        self.assertEqual(2,builder.id_counter["sum"])

import io
import re
import textwrap

import pytest

import openeo.processes
from openeo.api.process import Parameter
from openeo.internal.graph_building import (
    FlatGraphNodeIdGenerator,
    GraphFlattener,
    MultiResult,
    PGNode,
    PGNodeGraphUnflattener,
    ReduceNode,
)
from openeo.internal.process_graph_visitor import ProcessGraphVisitException
from openeo.rest.datacube import DataCube


def test_pgnode_process_id():
    assert PGNode("foo").process_id == "foo"


def test_pgnode_arguments():
    assert PGNode("foo").arguments == {}
    assert PGNode("foo", {"bar": 123}).arguments == {"bar": 123}
    assert PGNode("foo", arguments={"bar": 123}).arguments == {"bar": 123}
    assert PGNode("foo", bar=123).arguments == {"bar": 123}
    assert PGNode("foo", arguments={"bar": 123}, lol=456).arguments == {"bar": 123, "lol": 456}
    with pytest.raises(TypeError, match="multiple values for keyword argument 'bar"):
        PGNode("foo", arguments={"bar": 123}, bar=456)


def test_pgnode_namespace():
    assert PGNode("foo").namespace is None
    assert PGNode("foo", namespace="bar").namespace == "bar"


def test_pgnode_equality():
    assert PGNode("foo") == PGNode("foo")
    assert PGNode("foo") != PGNode("bar")
    assert PGNode("foo", {"x": 1}) == PGNode("foo", {"x": 1})
    assert PGNode("foo", {"x": 1}) == PGNode("foo", x=1)
    assert PGNode("foo", {"x": 1}) != PGNode("foo", {"y": 1})
    assert PGNode("foo", namespace="n1") == PGNode("foo", namespace="n1")
    assert PGNode("foo", namespace="n1") != PGNode("foo", namespace="b2")


def test_pgnode_to_dict():
    pg = PGNode(process_id="load_collection", arguments={"collection_id": "S2"})
    assert pg.to_dict() == {
        "process_id": "load_collection",
        "arguments": {"collection_id": "S2"}
    }


def test_pgnode_to_dict_namespace():
    pg = PGNode(process_id="load_collection", arguments={"collection_id": "S2"}, namespace="bar")
    assert pg.to_dict() == {
        "process_id": "load_collection",
        "namespace": "bar",
        "arguments": {"collection_id": "S2"}
    }


def test_pgnode_to_dict_nested():
    pg = PGNode(
        process_id="filter_bands",
        arguments={
            "bands": [1, 2, 3],
            "data": {"from_node": PGNode(
                process_id="load_collection",
                arguments={"collection_id": "S2"}
            )}
        }
    )
    assert pg.to_dict() == {
        'process_id': 'filter_bands',
        'arguments': {
            'bands': [1, 2, 3],
            'data': {'from_node': {
                'process_id': 'load_collection',
                'arguments': {'collection_id': 'S2'},
            }}
        },
    }


def test_pgnode_to_json_nested():
    pg = PGNode(
        process_id="filter_bands",
        arguments={
            "bands": [1, 2, 3],
            "data": {"from_node": PGNode(process_id="load_collection", arguments={"collection_id": "S2"})},
        },
    )
    assert pg.to_json() == textwrap.dedent(
        """\
        {
          "process_graph": {
            "loadcollection1": {
              "process_id": "load_collection",
              "arguments": {
                "collection_id": "S2"
              }
            },
            "filterbands1": {
              "process_id": "filter_bands",
              "arguments": {
                "bands": [
                  1,
                  2,
                  3
                ],
                "data": {
                  "from_node": "loadcollection1"
                }
              },
              "result": true
            }
          }
        }"""
    )


def test_pgnode_normalize_pgnode_args():
    graph = PGNode(
        "foo",
        x=PGNode("bar", color="red"),
        y={"from_node": PGNode("xev", color="green")},
    )
    assert graph.to_dict() == {
        "process_id": "foo",
        "arguments": {
            "x": {"from_node": {"process_id": "bar", "arguments": {"color": "red"}}},
            "y": {"from_node": {"process_id": "xev", "arguments": {"color": "green"}}},
        }
    }


def test_flat_graph_key_generate():
    g = FlatGraphNodeIdGenerator()
    assert g.generate("foo") == "foo1"
    assert g.generate("foo") == "foo2"
    assert g.generate("bar") == "bar1"
    assert g.generate("foo") == "foo3"


class TestGraphFlattener:
    def test_simple(self):
        node = PGNode("foo", bar="meh")
        flattener = GraphFlattener()
        assert flattener.flatten(node) == {"foo1": {"process_id": "foo", "arguments": {"bar": "meh"}, "result": True}}

    def test_chain(self):
        a = PGNode("a", bar="meh")
        b = PGNode("b", a=a)
        c = PGNode("c", a=a, b=b)
        flattener = GraphFlattener()
        assert flattener.flatten(c) == {
            "a1": {"process_id": "a", "arguments": {"bar": "meh"}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}},
            "c1": {
                "process_id": "c",
                "arguments": {"a": {"from_node": "a1"}, "b": {"from_node": "b1"}},
                "result": True,
            },
        }

    def test_no_multi_input_mode(self):
        a = PGNode("a")
        b = PGNode("b", a=a)
        flattener = GraphFlattener()
        flat_graph = flattener.flatten(a)
        assert flat_graph == {"a1": {"process_id": "a", "arguments": {}, "result": True}}
        with pytest.raises(RuntimeError, match="not in multi-input mode"):
            flattener.flatten(b)
        assert flat_graph == {"a1": {"process_id": "a", "arguments": {}, "result": True}}

    def test_multi_input_mode(self):
        a = PGNode("a")
        b = PGNode("b", a=a)
        c = PGNode("c", a=a)
        flattener = GraphFlattener(multi_input_mode=True)
        # Flatten b
        assert flattener.flatten(b) == {
            "a1": {"process_id": "a", "arguments": {}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}},
        }
        assert flattener.flattened() == {
            "a1": {"process_id": "a", "arguments": {}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}, "result": True},
        }
        # Flatten c
        assert flattener.flatten(c) == {
            "a1": {"process_id": "a", "arguments": {}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}},
            "c1": {"process_id": "c", "arguments": {"a": {"from_node": "a1"}}},
        }
        assert flattener.flattened() == {
            "a1": {"process_id": "a", "arguments": {}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}},
            "c1": {"process_id": "c", "arguments": {"a": {"from_node": "a1"}}, "result": True},
        }

    def test_multi_input_mode_mutation(self):
        """Verify that previously produced flat graphs are not silently mutated"""
        a = PGNode("a")
        b = PGNode("b", a=a)
        flattener = GraphFlattener(multi_input_mode=True)
        a_flat = flattener.flatten(a)
        assert a_flat == {
            "a1": {"process_id": "a", "arguments": {}},
        }
        b_flat = flattener.flatten(b)
        assert b_flat == {
            "a1": {"process_id": "a", "arguments": {}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}},
        }
        assert flattener.flattened() == {
            "a1": {"process_id": "a", "arguments": {}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}, "result": True},
        }
        # Original graphs are not mutated silently
        assert a_flat == {
            "a1": {"process_id": "a", "arguments": {}},
        }
        assert b_flat == {
            "a1": {"process_id": "a", "arguments": {}},
            "b1": {"process_id": "b", "arguments": {"a": {"from_node": "a1"}}},
        }


def test_build_and_flatten_simple():
    node = PGNode("foo")
    assert node.flat_graph() == {"foo1": {"process_id": "foo", "arguments": {}, "result": True}}


def test_build_and_flatten_arguments():
    node = PGNode("foo", bar="red", x=3)
    assert node.flat_graph() == {"foo1": {"process_id": "foo", "arguments": {"bar": "red", "x": 3}, "result": True}}


def test_build_and_flatten_argument_dict():
    node = PGNode("foo", {"bar": "red", "x": 3})
    assert node.flat_graph() == {"foo1": {"process_id": "foo", "arguments": {"bar": "red", "x": 3}, "result": True}}


def test_build_and_flatten_namespace():
    node = PGNode("foo", namespace="bar")
    assert node.flat_graph() == {"foo1": {"process_id": "foo", "namespace": "bar", "arguments": {}, "result": True}}


def test_pgnode_to_dict_subprocess_graphs():
    load_collection = PGNode("load_collection", collection_id="S2")
    band2 = PGNode("array_element", data={"from_parameter": "data"}, index=2)
    band2_plus3 = PGNode("add", x={"from_node": band2}, y=2)
    graph = ReduceNode(data=load_collection, reducer=band2_plus3, dimension="bands")

    assert graph.to_dict() == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {
                "from_node": {
                    "process_id": "load_collection",
                    "arguments": {"collection_id": "S2"},
                }
            },
            "dimension": "bands",
            "reducer": {
                "process_graph": {
                    "process_id": "add",
                    "arguments": {
                        "x": {
                            "from_node": {
                                "process_id": "array_element",
                                "arguments": {"data": {"from_parameter": "data"}, "index": 2},
                            }
                        },
                        "y": 2,
                    },
                }
            },
        },
    }
    assert graph.flat_graph() == {
        "loadcollection1": {
            'process_id': 'load_collection',
            'arguments': {'collection_id': 'S2'},
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "bands",
                "reducer": {
                    "process_graph": {
                        "arrayelement1": {
                            "process_id": "array_element",
                            "arguments": {"data": {"from_parameter": "data"}, "index": 2},
                        },
                        "add1": {
                            "process_id": "add",
                            "arguments": {"x": {"from_node": "arrayelement1"}, "y": 2},
                            "result": True,
                        },
                    }
                },
            },
            "result": True,
        }
    }


def test_reduce_node():
    a = PGNode("load_collection", collection_id="S2")
    graph = ReduceNode(a, reducer="mean", dimension="time")
    assert graph.to_dict() == {
        'process_id': 'reduce_dimension',
        'arguments': {
            'data': {'from_node': {
                'process_id': 'load_collection',
                'arguments': {'collection_id': 'S2'},
            }},
            'reducer': 'mean',
            'dimension': 'time',
        },
    }


def test_reduce_node_context():
    a = PGNode("load_collection", collection_id="S2")
    graph = ReduceNode(a, reducer="mean", dimension="time", context=123)
    assert graph.to_dict() == {
        'process_id': 'reduce_dimension',
        'arguments': {
            'data': {'from_node': {
                'process_id': 'load_collection',
                'arguments': {'collection_id': 'S2'},
            }},
            'reducer': 'mean',
            'dimension': 'time',
            "context": 123,
        },
    }


def test_reduce_node_process_graph():
    reduce_pg = PGNode("array_element", data={"from_parameter": "data"}, index=3)
    a = PGNode("load_collection", collection_id="S2")
    graph = ReduceNode(a, reducer=reduce_pg, dimension="time")
    assert graph.to_dict() == {
        'process_id': 'reduce_dimension',
        'arguments': {
            'data': {'from_node': {
                'process_id': 'load_collection',
                'arguments': {'collection_id': 'S2'},
            }},
            'reducer': {"process_graph": {
                "process_id": "array_element",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "index": 3
                }
            }},
            'dimension': 'time',
        },
    }


def test_pgnode_parameter_basic():
    pg = openeo.processes.add(x=Parameter.number("a", description="A."), y=42)
    assert pg.flat_graph() == {
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "a"}, "y": 42},
            "result": True
        }
    }


def test_pgnode_parameter_to_json():
    pg = openeo.processes.add(x=Parameter.number("a", description="A."), y=42)
    expected = '{"process_graph": {"add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "a"}, "y": 42}, "result": true}}}'
    assert pg.to_json(indent=None) == expected


def test_pgnode_parameter_print_json():
    pg = openeo.processes.add(x=Parameter.number("a", description="A."), y=42)
    out = io.StringIO()
    pg.print_json(file=out, indent=None)
    assert (
        out.getvalue()
        == '{"process_graph": {"add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "a"}, "y": 42}, "result": true}}}\n'
    )


def test_pgnode_parameter_fahrenheit():
    from openeo.processes import divide, subtract
    pg = divide(x=subtract(x=Parameter.number("f", description="Fahrenheit"), y=32), y=1.8)
    assert pg.flat_graph() == {
        "subtract1": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "f"}, "y": 32}},
        "divide1": {"process_id": "divide", "arguments": {"x": {"from_node": "subtract1"}, "y": 1.8}, "result": True},
    }


class TestPGNodeGraphUnflattener:

    def test_minimal(self):
        flat_graph = {
            "add12": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True},
        }
        result: PGNode = PGNodeGraphUnflattener.unflatten(flat_graph)
        assert result.process_id == "add"
        assert result.arguments == {"x": 1, "y": 2}
        assert result.namespace is None
        assert result == PGNode("add", {"x": 1, "y": 2})

        assert list(result.flat_graph().values()) == list(flat_graph.values())

    def test_basic(self):
        flat_graph = {
            "add12": {"process_id": "add", "arguments": {"x": 1, "y": 2}},
            "mul3": {"process_id": "multiply", "arguments": {"x": {"from_node": "add12"}, "y": 3}},
            "div4": {"process_id": "divide", "arguments": {"x": {"from_node": "mul3"}, "y": 4}, "result": True},
        }
        result: PGNode = PGNodeGraphUnflattener.unflatten(flat_graph)
        expected = PGNode("divide", x=PGNode("multiply", x=PGNode("add", x=1, y=2), y=3), y=4)
        assert result == expected

    def test_pgnode_reuse(self):
        flat_graph = {
            "value1": {"process_id": "constant", "arguments": {"x": 1}},
            "add1": {
                "process_id": "add",
                "arguments": {"x": {"from_node": "value1"}, "y": {"from_node": "value1"}},
                "result": True
            },
        }
        result: PGNode = PGNodeGraphUnflattener.unflatten(flat_graph)
        expected = PGNode("add", x=PGNode("constant", x=1), y=PGNode("constant", x=1))
        assert result == expected
        assert result.arguments["x"]["from_node"] is result.arguments["y"]["from_node"]

    def test_parameter_substitution_none(self):
        flat_graph = {
            "add": {"process_id": "add", "arguments": {"x": 1, "y": {"from_parameter": "increment"}}},
            "mul": {"process_id": "multiply", "arguments": {"x": {"from_node": "add"}, "y": 3}, "result": True},
        }
        result: PGNode = PGNodeGraphUnflattener.unflatten(flat_graph)
        expected = x = PGNode("multiply", x=PGNode("add", x=1, y={"from_parameter": "increment"}), y=3)
        assert result == expected

    def test_parameter_substitution_defined(self):
        flat_graph = {
            "add": {"process_id": "add", "arguments": {"x": 1, "y": {"from_parameter": "increment"}}},
            "mul": {"process_id": "multiply", "arguments": {"x": {"from_node": "add"}, "y": 3}, "result": True},
        }
        result: PGNode = PGNodeGraphUnflattener.unflatten(flat_graph, parameters={"increment": 100})
        expected = x = PGNode("multiply", x=PGNode("add", x=1, y=100), y=3)
        assert result == expected

    def test_parameter_substitution_undefined(self):
        flat_graph = {
            "add": {"process_id": "add", "arguments": {"x": 1, "y": {"from_parameter": "increment"}}},
            "mul": {"process_id": "multiply", "arguments": {"x": {"from_node": "add"}, "y": 3}, "result": True},
        }
        with pytest.raises(ProcessGraphVisitException, match="No substitution value for parameter 'increment'"):
            _ = PGNodeGraphUnflattener.unflatten(flat_graph, parameters={"other": 100})


def test_walk_nodes_basic():
    node = PGNode("foo")
    walk = node.walk_nodes()
    assert next(walk) is node
    with pytest.raises(StopIteration):
        next(walk)


def test_walk_nodes_args():
    data = PGNode("load")
    geometry = PGNode("vector")
    node = PGNode("foo", data=data, geometry=geometry)

    walk = node.walk_nodes()
    assert next(walk) is node
    rest = list(walk)
    assert rest == [data, geometry] or rest == [geometry, data]


def test_walk_nodes_nested():
    node = PGNode(
        "foo",
        cubes=[PGNode("load1"), PGNode("load2")],
        size={
            "x": PGNode("add", x=PGNode("five"), y=3),
            "y": PGNode("max"),
        },
    )
    walk = list(node.walk_nodes())
    assert all(isinstance(n, PGNode) for n in walk)
    assert set(n.process_id for n in walk) == {"load1", "max", "foo", "load2", "add", "five"}


class TestMultiResult:
    def test_simple(self):
        multi = MultiResult([PGNode("foo"), PGNode("bar")])
        assert multi.flat_graph() == {
            "foo1": {"process_id": "foo", "arguments": {}},
            "bar1": {"process_id": "bar", "arguments": {}, "result": True},
        }

    def test_simple_duplicates(self):
        multi = MultiResult([PGNode("foo"), PGNode("foo")])
        assert multi.flat_graph() == {
            "foo1": {"process_id": "foo", "arguments": {}},
            "foo2": {"process_id": "foo", "arguments": {}, "result": True},
        }

    def test_multi_save_result_same_root(self):
        load_collection = DataCube(PGNode("load_collection", collection_id="S2"))
        save_a = load_collection.save_result(format="GTiff")
        save_b = load_collection.save_result(format="NetCDF")
        multi = MultiResult([save_a, save_b])
        assert multi.flat_graph() == {
            "loadcollection1": {"process_id": "load_collection", "arguments": {"collection_id": "S2"}},
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
            },
            "saveresult2": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "NetCDF", "options": {}},
                "result": True,
            },
        }

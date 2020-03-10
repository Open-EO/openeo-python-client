import pytest

from openeo.internal.graphbuilder import GraphBuilder, FlatGraphKeyGenerator


def test_flat_graph_key_generate():
    g = FlatGraphKeyGenerator()
    assert g.generate("foo") == "foo1"
    assert g.generate("foo") == "foo2"
    assert g.generate("bar") == "bar1"
    assert g.generate("foo") == "foo3"


def test_add_process_simple():
    g = GraphBuilder()
    g.add_process("nope")
    assert g.result_node == {"process_id": "nope", "arguments": {}}


def test_add_process_arguments():
    g = GraphBuilder()
    g.add_process("flender", {"x": 3, "color": "green"})
    assert g.result_node == {"process_id": "flender", "arguments": {"color": "green", "x": 3}}


def test_add_process_kwargs():
    g = GraphBuilder()
    g.add_process("flender", x=5, color="red")
    assert g.result_node == {"process_id": "flender", "arguments": {"color": "red", "x": 5}}


def test_add_process_arguments_and_kwargs():
    g = GraphBuilder()
    with pytest.raises(ValueError):
        g.add_process("flender", {"x": 3, "color": "green"}, x=5, color="red")


def test_build_and_flatten_simple():
    b = GraphBuilder()
    b.add_process("foo")
    assert b.flatten() == {"foo1": {"process_id": "foo", "arguments": {}, "result": True}}


def test_build_and_flatten_arguments():
    b = GraphBuilder()
    b.add_process("foo", bar="red", x=3)
    assert b.flatten() == {"foo1": {"process_id": "foo", "arguments": {"bar": "red", "x": 3}, "result": True}}


def test_build_and_flatten_argument_dict():
    b = GraphBuilder()
    b.add_process("foo", {"bar": "red", "x": 3})
    assert b.flatten() == {"foo1": {"process_id": "foo", "arguments": {"bar": "red", "x": 3}, "result": True}}


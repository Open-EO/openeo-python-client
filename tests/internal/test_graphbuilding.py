import pytest

from openeo.internal.graph_building import FlatGraphNodeIdGenerator, PGNode, ReduceNode


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


def test_pgnode_to_dict():
    pg = PGNode(process_id="load_collection", arguments={"collection_id": "S2"})
    assert pg.to_dict() == {
        "process_id": "load_collection",
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


def test_build_and_flatten_simple():
    node = PGNode("foo")
    assert node.flatten() == {"foo1": {"process_id": "foo", "arguments": {}, "result": True}}


def test_build_and_flatten_arguments():
    node = PGNode("foo", bar="red", x=3)
    assert node.flatten() == {"foo1": {"process_id": "foo", "arguments": {"bar": "red", "x": 3}, "result": True}}


def test_build_and_flatten_argument_dict():
    node = PGNode("foo", {"bar": "red", "x": 3})
    assert node.flatten() == {"foo1": {"process_id": "foo", "arguments": {"bar": "red", "x": 3}, "result": True}}


def test_pgnode_to_dict_subprocess_graphs():
    load_collection = PGNode("load_collection", collection_id="S2")
    band2 = PGNode("array_element", data={"from_argument": "data"}, index=2)
    band2_plus3 = PGNode("add", x={"from_node": band2}, y=2)
    graph = ReduceNode(data=load_collection, reducer=band2_plus3, dimension='bands')

    assert graph.to_dict() == {
        'process_id': 'reduce_dimension',
        'arguments': {
            'data': {'from_node': {
                'process_id': 'load_collection',
                'arguments': {'collection_id': 'S2'},
            }},
            'dimension': 'bands',
            'reducer': {'process_graph': {
                'process_id': 'add',
                'arguments': {
                    'x': {"from_node": {
                        'process_id': 'array_element',
                        'arguments': {'data': {'from_argument': 'data'}, 'index': 2},
                    }},
                    'y': 2
                },
            }}
        },
    }
    assert graph.flatten() == {
        "loadcollection1": {
            'process_id': 'load_collection',
            'arguments': {'collection_id': 'S2'},
        },
        "reducedimension1": {
            'process_id': 'reduce_dimension',
            'arguments': {
                'data': {'from_node': "loadcollection1"},
                'dimension': 'bands',
                'reducer': {'process_graph': {
                    "arrayelement1": {
                        'process_id': 'array_element',
                        'arguments': {'data': {'from_argument': 'data'}, 'index': 2},
                    },
                    "add1": {
                        'process_id': 'add',
                        'arguments': {
                            'x': {"from_node": "arrayelement1"},
                            'y': 2
                        },
                        'result': True,
                    }}
                }
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


def test_reduce_node_process_graph():
    reduce_pg = PGNode("array_element", data={"from_argument": "data"}, index=3)
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
                    "data": {"from_argument": "data"},
                    "index": 3
                }
            }},
            'dimension': 'time',
        },
    }

from openeo.internal.graph_building import PGNode
from openeo.rest._datacube import build_child_callback, UDF


def test_build_child_callback_str():
    pg = build_child_callback("add", parent_parameters=["x", "y"])
    assert isinstance(pg["process_graph"], PGNode)
    assert pg["process_graph"].flat_graph() == {
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "x"}, "y": {"from_parameter": "y"}},
            "result": True,
        }
    }


def test_build_child_callback_pgnode():
    pg = build_child_callback(
        PGNode(process_id="add", arguments={"x": {"from_parameter": "x"}, "y": 1}), parent_parameters=["x"]
    )
    assert isinstance(pg["process_graph"], PGNode)
    assert pg["process_graph"].flat_graph() == {
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "x"}, "y": 1},
            "result": True,
        }
    }


def test_build_child_callback_lambda():
    pg = build_child_callback(lambda x: x + 1, parent_parameters=["x"])
    assert isinstance(pg["process_graph"], PGNode)
    assert pg["process_graph"].flat_graph() == {
        "add1": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "x"}, "y": 1},
            "result": True,
        }
    }


def test_build_child_callback_udf():
    pg = build_child_callback(UDF(code="def fun(x):\n    return x + 1", runtime="Python"), parent_parameters=["data"])
    assert isinstance(pg["process_graph"], PGNode)
    assert pg["process_graph"].flat_graph() == {
        "runudf1": {
            "process_id": "run_udf",
            "arguments": {
                "data": {"from_parameter": "data"},
                "runtime": "Python",
                "udf": "def fun(x):\n    return x + 1",
            },
            "result": True,
        }
    }
